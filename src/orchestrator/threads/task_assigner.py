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
import base64
from collections import deque
from datetime import datetime, timezone,timedelta

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

# Import from the neutral state file
from orchestrator.orchestrator_state import JobState

from core.db_models.job_schema import JobType
from core.models.models import TaskType, PolicySelectorPlanPayload, PolicyConfiguration, WorkPacketHeader, TaskConfig, WorkPacket, DiscoveryEnumeratePayload,BenchmarkExecutePayload,EntitlementExtractPayload,DatasourceProfilePayload
from core.errors import ErrorCategory
from core.utils.hash_utils import generate_task_id

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
        with an application-generated hash ID.
        """
        nodegroup = self.settings.system.node_group
        queued_jobs = await self.db.get_queued_jobs_for_nodegroup(nodegroup, limit=1)
        if not queued_jobs:
            return False

        job_to_claim = queued_jobs[0]
        job_id = job_to_claim.id
        
        lease_duration = 300
        was_claimed = await self.db.claim_queued_job(
            job_id, self.orchestrator.instance_id, lease_duration
        )

        if was_claimed:
            self.logger.info(
                f"Successfully claimed new '{job_to_claim.template_type.value}' job {job_id}.",
                job_id=job_id
            )
            await self.orchestrator._update_job_in_memory_state(
                job_id=job_id,
                new_status=JobState.RUNNING,
                version=job_to_claim.version + 1,
                lease_expiry=datetime.now(timezone.utc) + timedelta(seconds=lease_duration),
                is_new=True
            )
            
            # --- FIXED: Application-Side Task ID Generation ---
            
            # 1. Generate the unique task ID first.
            new_task_id_bytes = generate_task_id()
            new_task_id_hex = new_task_id_bytes.hex() # For the WorkPacket

            # 2. Create the WorkPacket with the new ID in the header.
            header = WorkPacketHeader(
                task_id=new_task_id_hex, # Use the hex string for the packet
                job_id=job_id
            )
            config = TaskConfig()
            
            # 3. Build the appropriate payload based on job type.
            if job_to_claim.template_type == JobType.SCANNING:
                datasource_id = job_to_claim.configuration.get('datasource_targets', [{}])[0].get('datasource_id')
                if not datasource_id:
                    await self.db.fail_job(job_id, "Job is missing a datasource_id.")
                    return False

                # FIXED: Staging table logic now respects discovery_mode
                discovery_mode = job_to_claim.configuration.get("discovery_mode", "FULL").upper()
                staging_table = "discovered_objects"
                if discovery_mode == "DELTA":
                    staging_table = f"staging_discovered_objects_job_{job_id}"
                    await self.db.create_staging_table_for_job(job_to_claim.id, "DiscoveredObjects")
                    
                payload = DiscoveryEnumeratePayload(
                    datasource_id=datasource_id,
                    paths=[],
                    staging_table_name=staging_table
                )
                task_type = TaskType.DISCOVERY_ENUMERATE
            elif job_to_claim.template_type == JobType.DB_PROFILE:
                task_type = TaskType.DATASOURCE_PROFILE
                payload = DatasourceProfilePayload(datasource_id=datasource_id)

            elif job_to_claim.template_type == JobType.BENCHMARK:
                task_type = TaskType.BENCHMARK_EXECUTE
                profile_record = await self.db.get_datasource_metadata(datasource_id)
                


                payload = BenchmarkExecutePayload(
                    datasource_id=datasource_id,
                    cycle_id=job_to_claim.get("cycle_id"),
                    benchmark_name=job_to_claim.get("benchmark_name"),
                    
                )

            elif job_to_claim.template_type == JobType.ENTITLEMENT:
                task_type = TaskType.ENTITLEMENT_EXTRACT
                payload = EntitlementExtractPayload(
                    datasource_id=datasource_id,
                    cycle_id=job_to_claim.configuration.get("cycle_id")
                )                
            elif job_to_claim.template_type == JobType.POLICY:
                plan_id = f"plan_{job_to_claim.execution_id}"
                policy_config = PolicyConfiguration(**job_to_claim.configuration.get("policy_definition", {}))
                payload = PolicySelectorPlanPayload(plan_id=plan_id, policy_config=policy_config)
                task_type = TaskType.POLICY_SELECTOR_PLAN
            
            else:
                await self.db.fail_job(job_id, f"Unknown job type '{job_to_claim.template_type.value}'")
                return False

            full_work_packet = WorkPacket(header=header, config=config, payload=payload)

            # 4. Call the updated create_task method with the bytes hash.
            await self.db.create_task(
                job_id=job_id,
                task_id=new_task_id_bytes, # Pass the raw bytes to the DB method
                task_type=task_type,
                work_packet=full_work_packet.model_dump(mode='json'),
                datasource_id=getattr(payload, 'datasource_id', None)
            )
            
            await self._refill_task_cache_for_job(job_id)
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
        # Handle both bytes and string task_id
        task_id_str = task.id.hex() if isinstance(task.id, bytes) else task.id
        task_id_bytes = task.id if isinstance(task.id, bytes) else bytes.fromhex(task.id)
        
        worker_id = f"worker_for_task_{task_id_str}"
        
        was_assigned = await self.db.assign_task_to_worker(
            task_id_bytes,
            worker_id,
            self.settings.worker.task_timeout_seconds
        )
        
        if was_assigned:
            # --- START OF CRITICAL FIX ---
            # The task object from the DB has the correct, incremented retry_count.
            # We must now inject this into the work_packet's config before dispatching it.
            # This is the ONLY way the worker knows it's a recovery operation.
            task.work_packet['config']['retry_count'] = task.retry_count
            # --- END OF CRITICAL FIX ---

            self.rc.state_manager.confirm_task_assignment(task.job_id)
            self.logger.info(f"Task {task_id_str} successfully assigned.", task_id=task_id_str)
            
            if self.orchestrator.is_single_process_mode:
                if self.orchestrator._in_process_work_queue is None:
                    self.logger.error("In-process work queue is None!")
                    return                
                await self.orchestrator._in_process_work_queue.put(task)
        else:
            self.logger.warning(f"DB assignment for task {task_id_str} failed (race condition).", task_id=task_id_str)
            if hasattr(task, 'datasource_id') and task.datasource_id:
                self.rc.state_manager.release_datasource_connection(task.datasource_id)


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