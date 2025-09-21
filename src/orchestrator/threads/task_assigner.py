# src/orchestrator/threads/task_assigner.py
"""
The TaskAssigner is the core logic coroutine of the Orchestrator. It is
responsible for matching pending tasks with available resources and workers,
implementing a robust scheduling, caching, and leasing strategy.

FIXES APPLIED:
- Updated to use unified job_cache from orchestrator (no separate task cache)
- Fixed state access to work with new unified dictionary structure
- Fixed schema compliance: uses task.datasource_id and task.job_id
- Updated to use existing get_jobs_by_ids database method
"""

import asyncio
import time
from collections import deque
from datetime import datetime, timezone,timedelta

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

# Import from the neutral state file
from orchestrator.orchestrator_state import JobState

from core.db_models.job_schema import JobType
from core.models.models import TaskType, PolicySelectorPlanPayload, PolicyConfiguration, WorkPacketHeader, TaskConfig,WorkPacket
from core.errors import ErrorCategory
class TaskAssigner:
    """Finds, approves, and dispatches tasks, respecting all system constraints."""
    
    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.rc = orchestrator.resource_coordinator
        self.settings = orchestrator.settings
        self.interval = orchestrator.settings.orchestrator.task_assigner_interval_sec
        self.name = "TaskAssignerCoroutine"
        
        # NOTE: Using orchestrator's unified job_cache instead of separate cache
        # All task caching operations will use self.orchestrator.job_cache


    async def run_async(self):
        """The main async loop for the Task Assigner coroutine."""
        self.logger.log_component_lifecycle("TaskAssigner", "STARTED")
        backoff_delay = self.interval

        while not self.orchestrator._shutdown_event.is_set():
            try:
                # BACKPRESSURE CHECK (for single_process mode)
                if self.orchestrator.is_single_process_mode:
                    q_size = self.orchestrator._in_process_work_queue.qsize()
                    # Threshold should be configurable, e.g., 2x worker count
                    if q_size > (self.settings.worker.in_process_thread_count * 2):
                        self.logger.warning(f"Task queue is full ({q_size} items). Throttling task assignment.", queue_size=q_size)
                        await asyncio.sleep(backoff_delay)
                        backoff_delay = min(backoff_delay * 1.5, 60) # Exponential backoff up to 60s
                        continue
                
                backoff_delay = self.interval # Reset backoff on successful check

                # Continuously try to find and dispatch the next available task
                task_to_dispatch = await self._find_and_approve_task()
                if task_to_dispatch:
                    await self._dispatch_task(task_to_dispatch)
                else:
                    await asyncio.sleep(self.interval)
                
                self.orchestrator.update_thread_liveness("task_assigner")
            
            except Exception as e:
                error = self.orchestrator.error_handler.handle_error(e, "task_assigner_main_loop")
                
                # Fatal error detection and propagation
                if error.error_category == ErrorCategory.FATAL_BUG:
                    self.logger.critical(f"Fatal error detected in {self.__class__.__name__}: {error}")
                    raise  # Propagate to TaskManager
                
                # Existing retry logic for operational errors
                self.logger.warning(f"Transient error in {self.__class__.__name__}, retrying: {error}")
                await asyncio.sleep(self.interval * 5)

        self.logger.log_component_lifecycle("TaskAssigner", "STOPPED")

    async def _claim_and_initialize_new_job(self) -> bool:
        """
        Finds a single QUEUED job, claims it, and creates the correct first task
        based on the job's template_type.
        Returns True if a job was successfully initialized, otherwise False.
        """
        nodegroup = self.settings.system.node_group
        # Find one available job, prioritizing by the 'priority' and then 'id' column
        queued_jobs = await self.db.get_queued_jobs_for_nodegroup(nodegroup, limit=1)
        if not queued_jobs:
            return False

        job_to_claim = queued_jobs[0]
        
        lease_duration = 300 # seconds
        was_claimed = await self.db.claim_queued_job(
            job_to_claim.id, self.orchestrator.instance_id, lease_duration
        )

        if was_claimed:
            self.logger.info(
                f"Successfully claimed new '{job_to_claim.template_type.value}' job {job_to_claim.id}.",
                job_id=job_to_claim.id
            )
            # Update in-memory state
            self.logger.info(f"[DEBUG] taskassigner about to call _update_job_in_memory_state")
            await self.orchestrator._update_job_in_memory_state(
                job_id=job_to_claim.id,
                new_status=JobState.RUNNING,
                version=job_to_claim.version + 1,
                lease_expiry=datetime.now(timezone.utc) + timedelta(seconds=lease_duration),
                is_new=True
            )
            
            #
            # --- Type-Aware First Task Creation Logic ---
            #
            if job_to_claim.template_type == JobType.SCANNING:
                # --- FIX STARTS HERE ---
                datasource_targets = job_to_claim.configuration.get('datasource_targets', [])
                
                # A child job is scoped to a single datasource, so we can safely use the first one.
                datasource_id = datasource_targets[0]['datasource_id'] if datasource_targets else None
                if not datasource_id:
                    # If a scanning job has no datasource, it's an unrecoverable configuration error.
                    error_msg = f"Scanning Job {job_to_claim.id} is missing a datasource_id in its configuration."
                    self.logger.error(error_msg, job_id=job_to_claim.id)
                    await self.db.fail_job(job_to_claim.id, error_msg)
                    return False

                # --- FIX IS HERE ---
                # Build a full, valid WorkPacket dictionary
                header = WorkPacketHeader(task_id=0, job_id=job_to_claim.id) # task_id will be populated by DB
                config = TaskConfig() # Use default config
                payload = {
                     "task_type": TaskType.DISCOVERY_ENUMERATE,
                     "paths": [],
                     "datasource_id": datasource_id,
                     "staging_table_name": f"staging_discovered_objects_job_{job_to_claim.id}"
                }
                
                # Create the final dictionary to be stored as JSON
                full_work_packet_model = WorkPacket(
                    header=header,
                    config=config,
                    payload=payload
                )
                # Use Pydantic's model_dump method to get a clean JSON string,
                
                import json
                work_packet_dict = full_work_packet_model.model_dump(mode='json')
                await self.db.create_task(
                    job_id=job_to_claim.id,
                    task_type=TaskType.DISCOVERY_ENUMERATE,
                    work_packet=work_packet_dict, # Pass the complete structure
                    datasource_id=datasource_id
                )
                # --- FIX ENDS HERE ---            
            elif job_to_claim.template_type == JobType.POLICY:
                # Create the initial task for a POLICY job
                # The first step in a policy workflow is to plan the selection.
                plan_id = f"plan_{job_to_claim.execution_id}"
                policy_config = PolicyConfiguration(**job_to_claim.configuration.get("policy_definition", {}))
                
                payload = PolicySelectorPlanPayload(
                    plan_id=plan_id,
                    policy_config=policy_config
                )
                await self.db.create_task(
                    job_id=job_to_claim.id,
                    task_type=TaskType.POLICY_SELECTOR_PLAN,
                    work_packet={"payload": payload.dict()}
                )
            
            else:
                # Fail the job if we don't know how to start it.
                error_msg = f"Unknown job type '{job_to_claim.template_type.value}'. Cannot create initial task."
                self.logger.error(error_msg, job_id=job_to_claim.id)
                await self.db.fail_job(job_to_claim.id, error_msg)
                return False

            # Refill the cache so the newly created task is available immediately
            await self._refill_task_cache_for_job(job_to_claim.id)
            return True
            
        return False




    async def _find_and_approve_task(self):
        """
        Finds a runnable job, gets a pending task from its cache, 
        and reserves resources for it.
        """
        # This function now correctly identifies a job and ensures its cache is full.
        job_to_run = await self._select_next_job_to_process()
        
        if not job_to_run: 
            return None # No runnable jobs were found or could be initialized.

        # --- FIX STARTS HERE ---
        # The cache for job_to_run should have been refilled by the selection logic.
        # Now, we simply get a task from it.
        task = self._get_task_from_cache(job_to_run.id)
        
        if not task:
            # This can happen if the refill found no PENDING tasks for the job.
            # The job might be finishing up. This is a normal condition.
            return None

        # Verify the lease one last time before dispatching.
        async with self.orchestrator._state_lock:
            job_state_info = self.orchestrator.job_states.get(job_to_run.id)
            if not job_state_info or job_state_info.get('lease_expiry') < datetime.now(timezone.utc):
                self.logger.warning(f"In-memory lease expired for job {job_to_run.id} just before task dispatch.", job_id=job_to_run.id)
                self.orchestrator.job_cache[job_to_run.id].appendleft(task) # Put task back
                return None

        # Now proceed with resource reservation as before.
        datasource_id_to_check = task.datasource_id
        if not datasource_id_to_check:
            try:
                datasource_id_to_check = task.work_packet['payload']['datasource_ids'][0]
            except (KeyError, IndexError):
                datasource_id_to_check = None
        
        decision_approved = False
        decision_reason = "No datasource-specific resources required."

        if datasource_id_to_check:
            decision = await self.rc.reserve_resources_for_task(datasource_id_to_check)
            decision_approved = decision.is_approved
            decision_reason = decision.reason
        else:
            decision_approved = True

        if decision_approved:
            return task
        else:
            self.orchestrator.job_cache[job_to_run.id].appendleft(task) # Put task back
            async with self.orchestrator._state_lock:
                self.orchestrator.snoozed_jobs[job_to_run.id] = time.monotonic() + 60
            self.logger.info(f"Snoozing job {job_to_run.id}: {decision_reason}", job_id=job_to_run.id)
            return None

    def _get_task_from_cache(self, job_id: int):
        """Gets a single task from the orchestrator's unified cache for a given job."""
        if job_id in self.orchestrator.job_cache and self.orchestrator.job_cache[job_id]:
            return self.orchestrator.job_cache[job_id].popleft()
        return None

    async def _refill_task_cache_for_job(self, job_id: int):
        """Fetches a new batch of pending tasks from the DB for a specific job."""
        async with self.orchestrator._state_lock:
            # Double-check that the cache is still empty to avoid a race condition
            if not (job_id in self.orchestrator.job_cache and self.orchestrator.job_cache[job_id]):
                tasks = await self.db.get_pending_tasks_batch(job_id, self.settings.orchestrator.task_cache_size)
                if tasks:
                    self.orchestrator.job_cache[job_id] = deque(tasks)
                    self.logger.info(f"Refilled task cache for job {job_id} with {len(tasks)} tasks.", job_id=job_id)

    async def _dispatch_task(self, task):
        """Assigns a task in the DB and dispatches it."""
        # This assignment is for a worker process in a real deployment.
        # For single_process mode, the "worker" is a coroutine in the same process.
        worker_id = f"worker_for_task_{task.id}" 
        
        was_assigned = await self.db.assign_task_to_worker(
            task.id, 
            worker_id,
            self.settings.worker.task_timeout_seconds
        )
        
        if was_assigned:
            # FIXED: Use task.job_id instead of task.JobID
            self.rc.state_manager.confirm_task_assignment(task.job_id)
            self.logger.info(f"Task {task.id} successfully assigned.", task_id=task.id)
            
            if self.orchestrator.is_single_process_mode:
                if self.orchestrator._in_process_work_queue is None:
                    self.logger.error("In-process work queue is None in single_process mode!")
                    return                
                await self.orchestrator._in_process_work_queue.put(task)
            # In EKS mode, the worker would have already received the task via the API,
            # and this DB update confirms the assignment.
        else:
            self.logger.warning(f"DB assignment for task {task.id} failed (race condition). Releasing reservation.", task_id=task.id)
            # In a real system, you would need to check if task.datasource_id exists
            if hasattr(task, 'datasource_id') and task.datasource_id:
                self.rc.state_manager.release_task_reservation(task.datasource_id)



    async def _select_next_job_to_process(self):
        """
        Selects the best job to assign a task from using a capacity-aware,
        pull-based model with consolidated state locking for efficiency.
        """
        # STEP 1: Consolidate state access into a single lock acquisition.
        async with self.orchestrator._state_lock:
            now = time.monotonic()
            
            # Get the current list of owned, runnable jobs.
            owned_job_ids = [
                job_id for job_id, state in self.orchestrator.job_states.items()
                if state.get('status') == JobState.RUNNING and self.orchestrator.snoozed_jobs.get(job_id, 0) < now
            ]
            
            # Calculate the current workload based on tasks already in memory.
            current_workload = sum(len(cache) for job_id, cache in self.orchestrator.job_cache.items() if job_id in owned_job_ids)
            
            # Define the capacity threshold.
            workload_threshold = self.settings.orchestrator.task_cache_size * 2

        # STEP 2: Decide whether to claim a new job based on capacity.
        job_was_claimed = False
        if not owned_job_ids or current_workload < workload_threshold:
            # The method returns True if a new job was successfully claimed and initialized.
            job_was_claimed = await self._claim_and_initialize_new_job()

        # STEP 3: If a new job was claimed, we must refresh our view of owned jobs.
        if job_was_claimed:
            async with self.orchestrator._state_lock:
                owned_job_ids = [
                    job_id for job_id, state in self.orchestrator.job_states.items()
                    if state.get('status') == JobState.RUNNING
                ]

        if not owned_job_ids:
            return None # No work to do.

        active_jobs = await self.db.get_jobs_by_ids(owned_job_ids)
        if not active_jobs:
            return None
            
        # STEP 4: Refill ALL empty caches in parallel.
        refill_coroutines = []
        for job in active_jobs:
            # Check if cache is missing or empty.
            if not self.orchestrator.job_cache.get(job.id):
                refill_coroutines.append(self._refill_task_cache_for_job(job.id))
        
        if refill_coroutines:
            self.logger.info(f"Refilling task caches for {len(refill_coroutines)} jobs in parallel.")
            await asyncio.gather(*refill_coroutines)

        # STEP 5: With caches now full, select the next task fairly.
        jobs_with_tasks_in_cache = [
            job for job in active_jobs if self.orchestrator.job_cache.get(job.id)
        ]

        if jobs_with_tasks_in_cache:
            return self.rc.get_next_job_for_assignment(jobs_with_tasks_in_cache)

        return None    