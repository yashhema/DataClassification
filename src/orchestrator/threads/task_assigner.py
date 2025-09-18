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
from datetime import datetime, timezone

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

# Import from the neutral state file
from orchestrator.orchestrator_state import JobState
import uuid
from core.db_models.job_schema import JobType
from core.models.models import TaskType, PolicySelectorPlanPayload, PolicyConfiguration
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
                self.orchestrator.error_handler.handle_error(e, "task_assigner_main_loop")
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
                # Create the initial task for a standard SCANNING job
                datasource_targets = job_to_claim.configuration.get('datasource_targets', [])
                initial_payload = {
                    "datasource_ids": [ds['datasource_id'] for ds in datasource_targets],
                    "staging_table_name": f"staging_discovered_objects_job_{job_to_claim.id}"
                }
                await self.db.create_task(
                    job_id=job_to_claim.id,
                    task_type=TaskType.DISCOVERY_ENUMERATE,
                    work_packet={"payload": initial_payload}
                )
            
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
        Finds a runnable job, verifies its lease, gets a pending task, 
        and reserves resources for it.
        """
        
        # 1. Select the best job to work on from the currently owned jobs.
        job_to_run = await self._select_next_job_to_process()
        
        if not job_to_run: 
            return None # No runnable jobs with pending tasks are owned by this orchestrator

        # 2. VERIFY LEASE: Check the in-memory lease before proceeding.
        async with self.orchestrator._state_lock:
            job_state_info = self.orchestrator.job_states.get(job_to_run.id)
            # FIXED: Access lease_expiry from the unified state dictionary
            if not job_state_info or job_state_info.get('lease_expiry') < datetime.now(timezone.utc):
                self.logger.warning(
                    f"In-memory lease expired for job {job_to_run.id}. Triggering renewal process.", 
                    job_id=job_to_run.id
                )
                # Abort this attempt. The heartbeat/renewal process will handle re-acquiring the lease.
                return None

        # 3. Get a task from that job's cache (using orchestrator's unified cache).
        task = self._get_task_from_cache(job_to_run.id)
        
        if not task:
            # This is normal; it just means the cache is empty and needs to be refilled.
            await self._refill_task_cache_for_job(job_to_run.id)
            return None

        # 4. Reserve resources for this specific task (the final gatekeeper step).
        # FIXED: Use task.datasource_id instead of task.DatasourceID
        decision = await self.rc.reserve_resources_for_task(task.datasource_id)
        
        if decision.is_approved:
            return task
        else:
            # If denied, put the task back at the front of the queue and snooze the job.
            self.orchestrator.job_cache[job_to_run.id].appendleft(task)
            async with self.orchestrator._state_lock:
                self.orchestrator.snoozed_jobs[job_to_run.id] = time.monotonic() + decision.snooze_duration_sec
            self.logger.info(f"Snoozing job {job_to_run.id}: {decision.reason}", job_id=job_to_run.id)
            return None

    async def _select_next_job_to_process(self):
        """
        Selects the best job to assign a task from, based on priority and fairness.
        If no currently owned jobs have tasks, it attempts to claim and initialize a new one.
        """
        async with self.orchestrator._state_lock:
            now = time.monotonic()
            # Get a list of all job IDs this orchestrator owns that are in a runnable state and not snoozed
            owned_job_ids = [
                job_id for job_id, state in self.orchestrator.job_states.items()
                if state.get('status') == JobState.RUNNING and self.orchestrator.snoozed_jobs.get(job_id, 0) < now
            ]
        
        # If we don't own any runnable jobs, try to claim a new one
        if not owned_job_ids:
            await self._claim_and_initialize_new_job()
            return None # Return None to allow the main loop to cycle

        # Fetch the full job objects from the DB to get priority and other details
        active_jobs = await self.db.get_jobs_by_ids(owned_job_ids) [cite: 1960]
        
        # Filter for jobs that currently have tasks in their in-memory cache
        jobs_with_tasks = [job for job in active_jobs if job.id in self.orchestrator.job_cache and self.orchestrator.job_cache[job.id]] [cite: 1961]
        
        #
        # --- Main Selection Logic ---
        #
        if jobs_with_tasks:
            # If we have jobs with ready tasks, use the Resource Coordinator's fairness
            # logic to pick the best one to process next.
            return self.rc.get_next_job_for_assignment(jobs_with_tasks) [cite: 1962]
        else:
            # If none of our currently owned jobs have tasks in their cache,
            # it's an opportune moment to check for new work.
            await self._claim_and_initialize_new_job()
            return None # Return None to allow the main loop to cycle

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
            self.rc.confirm_task_assignment(task.job_id)
            self.logger.info(f"Task {task.id} successfully assigned.", task_id=task.id)
            
            if self.orchestrator.is_single_process_mode:
                await self.orchestrator._in_process_work_queue.put(task)
            # In EKS mode, the worker would have already received the task via the API,
            # and this DB update confirms the assignment.
        else:
            self.logger.warning(f"DB assignment for task {task.id} failed (race condition). Releasing reservation.", task_id=task.id)
            # FIXED: Use task.datasource_id instead of task.DatasourceID
            self.rc.release_task_reservation(task.datasource_id)