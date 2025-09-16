# src/orchestrator/threads/task_assigner.py
"""
The TaskAssigner is the core logic coroutine of the Orchestrator. It is
responsible for matching pending tasks with available resources and workers,
implementing a robust scheduling, caching, and leasing strategy.
"""

import asyncio
import time
from collections import deque
from datetime import datetime, timezone

# Import for type hinting
from typing import TYPE_CHECKING, Dict
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

# Import from the new, neutral state file
from orchestrator.orchestrator_state import JobState

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
        
        # The new in-memory cache to hold task queues for multiple jobs
        self.job_task_cache: Dict[int, deque] = {}

    async def run_async(self):
        """The main async loop for the Task Assigner coroutine."""
        self.logger.log_component_lifecycle("TaskAssigner", "STARTED")
        
        while not self.orchestrator._shutdown_event.is_set():
            try:
                # Continuously try to find and dispatch the next available task
                task_to_dispatch = await self._find_and_approve_task()
                if task_to_dispatch:
                    await self._dispatch_task(task_to_dispatch)
                else:
                    # If no tasks are available or approved, wait before retrying
                    await asyncio.sleep(self.interval)
                
                self.orchestrator.update_thread_liveness("task_assigner")
            
            except Exception as e:
                self.orchestrator.error_handler.handle_error(e, "task_assigner_main_loop")
                await asyncio.sleep(self.interval * 5) # Back off on repeated failures

        self.logger.log_component_lifecycle("TaskAssigner", "STOPPED")

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
            if not job_state_info or job_state_info.get('lease_expiry') < datetime.now(timezone.utc):
                self.logger.warning(
                    f"In-memory lease expired for job {job_to_run.id}. Triggering renewal process.", 
                    job_id=job_to_run.id
                )
                # Abort this attempt. The heartbeat/renewal process will handle re-acquiring the lease.
                return None

        # 3. Get a task from that job's in-memory cache.
        task = self._get_task_from_cache(job_to_run.id)
        
        if not task:
            # This is normal; it just means the cache is empty and needs to be refilled.
            await self._refill_task_cache_for_job(job_to_run.id)
            return None

        # 4. Reserve resources for this specific task (the final gatekeeper step).
        decision = await self.rc.reserve_resources_for_task(task.DatasourceID)
        
        if decision.is_approved:
            return task
        else:
            # If denied, put the task back at the front of the queue and snooze the job.
            self.job_task_cache[job_to_run.id].appendleft(task)
            async with self.orchestrator._state_lock:
                self.orchestrator.snoozed_jobs[job_to_run.id] = time.monotonic() + decision.snooze_duration_sec
            self.logger.info(f"Snoozing job {job_to_run.id}: {decision.reason}", job_id=job_to_run.id)
            return None

    async def _select_next_job_to_process(self):
        """
        Selects the best job to assign a task from, based on priority and fairness.
        It also fetches new queued jobs if no work is currently assigned.
        """
        async with self.orchestrator._state_lock:
            now = time.monotonic()
            # Get a list of jobs this orchestrator owns that are running and not snoozed
            owned_job_ids = [
                job_id for job_id, state in self.orchestrator.job_states.items()
                if state.get('status') == JobState.RUNNING and self.orchestrator.snoozed_jobs.get(job_id, 0) < now
            ]
        
        # Fetch full job objects from DB to get priority info
        if not owned_job_ids:
            return None
        
        active_jobs = await self.db.get_jobs_by_ids(owned_job_ids)
        
        # Filter for jobs that have tasks in the cache (or could have tasks)
        jobs_with_tasks = [job for job in active_jobs if job.id in self.job_task_cache and self.job_task_cache[job.id]]
        
        if not jobs_with_tasks:
            return None

        # Logic for job selection: Highest priority first, then fewest active workers as a tie-breaker.
        return self.rc.get_next_job_for_assignment(jobs_with_tasks)

    def _get_task_from_cache(self, job_id: int):
        """Gets a single task from the in-memory cache for a given job."""
        if job_id in self.job_task_cache and self.job_task_cache[job_id]:
            return self.job_task_cache[job_id].popleft()
        return None

    async def _refill_task_cache_for_job(self, job_id: int):
        """Fetches a new batch of pending tasks from the DB for a specific job."""
        async with self.orchestrator._state_lock:
            # Double-check that the cache is still empty to avoid a race condition
            if not (job_id in self.job_task_cache and self.job_task_cache[job_id]):
                tasks = await self.db.get_pending_tasks_batch(job_id, self.settings.orchestrator.task_cache_size)
                if tasks:
                    self.job_task_cache[job_id] = deque(tasks)
                    self.logger.info(f"Refilled task cache for job {job_id} with {len(tasks)} tasks.", job_id=job_id)

    async def _dispatch_task(self, task):
        """Assigns a task in the DB and dispatches it."""
        # This assignment is for a worker process in a real deployment.
        # For single_process mode, the "worker" is a coroutine in the same process.
        worker_id = f"worker_for_task_{task.ID}" 
        
        was_assigned = await self.db.assign_task_to_worker(
            task.ID, 
            worker_id,
            self.settings.worker.task_timeout_seconds
        )
        
        if was_assigned:
            self.rc.confirm_task_assignment(task.JobID)
            self.logger.info(f"Task {task.ID} successfully assigned.", task_id=task.ID)
            
            if self.orchestrator.is_single_process_mode:
                await self.orchestrator._in_process_work_queue.put(task)
            # In EKS mode, the worker would have already received the task via the API,
            # and this DB update confirms the assignment.
        else:
            self.logger.warning(f"DB assignment for task {task.ID} failed (race condition). Releasing reservation.", task_id=task.ID)
            self.rc.release_task_reservation(task.DatasourceID)