# src/orchestrator/threads/task_assigner.py
"""
The TaskAssigner is the core logic thread of the Orchestrator. It is
responsible for matching pending tasks with available resources and workers,
implementing the robust "reserve-confirm-release" two-phase commit pattern
to ensure state consistency and prevent resource leaks.

FIXES APPLIED:
- Updated lock references to use orchestrator._state_lock (single lock pattern)
- Consolidated lock usage to prevent race conditions
- Maintained existing method calls (they work correctly with resource_coordinator)
"""

import threading
import time
import queue
from collections import deque

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class TaskAssigner(threading.Thread):
    """Finds, approves, and dispatches tasks, respecting all system constraints."""
    
    def __init__(self, orchestrator: "Orchestrator"):
        super().__init__(name="TaskAssignerThread", daemon=True)
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.rc = orchestrator.resource_coordinator
        self.settings = orchestrator.settings
        self.interval = orchestrator.settings.orchestrator.task_assigner_interval_sec

    def run(self):
        """The main loop for the Task Assigner thread."""
        self.logger.log_component_lifecycle("TaskAssigner", "STARTED")
        
        while not self.orchestrator._shutdown_event.is_set():
            try:
                task_to_dispatch = self._find_and_approve_task()
                if task_to_dispatch:
                    self._dispatch_task(task_to_dispatch)
                else:
                    # No tasks found or approved, wait before retrying
                    time.sleep(self.interval)
                
                # Update the liveness timestamp to show the thread is healthy
                self.orchestrator.update_thread_liveness("task_assigner")
            
            except Exception as e:
                self.logger.error("An unexpected error occurred in the TaskAssigner loop.", exc_info=True)
                time.sleep(self.interval)

        self.logger.log_component_lifecycle("TaskAssigner", "STOPPED")

    def _find_and_approve_task(self):
        """Finds a runnable job, gets a pending task, and reserves resources for it."""
        active_jobs = self.db.get_active_jobs()
        
        # FIXED: Use single lock pattern from refactored orchestrator
        with self.orchestrator._state_lock:
            # Filter out any job that is not in a RUNNING state
            runnable_jobs = [j for j in active_jobs if self.orchestrator.job_states.get(j.id, "RUNNING") == "RUNNING"]

            now = time.monotonic()
            # Further filter out any job that is temporarily snoozed due to a resource denial
            runnable_jobs = [j for j in runnable_jobs if self.orchestrator.snoozed_jobs.get(j.id, 0) < now]
            
        if not runnable_jobs: 
            return None

        job_to_run = self.rc.get_next_job_for_assignment(runnable_jobs)
        if not job_to_run: 
            return None
        
        job_id = job_to_run.id
        task = None
        
        # FIXED: Use single lock for job cache access
        with self.orchestrator._state_lock:
            # Refill the in-memory task cache for the job if it's empty
            if job_id not in self.orchestrator.job_cache or not self.orchestrator.job_cache[job_id]:
                tasks = self.db.get_pending_tasks_batch(job_id, self.settings.orchestrator.task_cache_size)
                if not tasks: 
                    return None
                self.orchestrator.job_cache[job_id] = deque(tasks)
            
            if not self.orchestrator.job_cache[job_id]: 
                return None
            task = self.orchestrator.job_cache[job_id].popleft()

        if not task or not task.DatasourceID:
            return None

        # --- Reserve Step ---
        decision = self.rc.reserve_resources_for_task(task.DatasourceID)
        
        if decision.is_approved:
            return task
        else:
            # If denied, put the task back and snooze the job to prevent a hot loop
            with self.orchestrator._state_lock:
                self.orchestrator.job_cache[job_id].appendleft(task)
                self.orchestrator.snoozed_jobs[job_id] = time.monotonic() + decision.snooze_duration_sec
            self.logger.info(f"Snoozing job {job_id}: {decision.reason}", job_id=job_id)
            return None

    def _dispatch_task(self, task):
        """Assigns a task in the DB and dispatches it, confirming or releasing the reservation."""
        worker_id, response_queue = None, None
        was_assigned = False
        
        try:
            if self.orchestrator.is_single_process_mode:
                worker_id = f"in-process-worker-task-{task.ID}"
                was_assigned = self.db.assign_task_to_worker(task.ID, worker_id, self.settings.worker.task_timeout_seconds)
            else: # EKS Mode
                try:
                    worker_id, response_queue = self.orchestrator.waiting_workers.get_nowait()
                    was_assigned = self.db.assign_task_to_worker(task.ID, worker_id, self.settings.worker.task_timeout_seconds)
                except queue.Empty:
                    self.logger.info("Task approved, but no workers were waiting. Releasing reservation.", task_id=task.ID)
                    self.rc.release_task_reservation(task.DatasourceID)
                    with self.orchestrator._state_lock: 
                        self.orchestrator.job_cache[task.JobID].appendleft(task)
                    return

            # --- Confirm/Release Step ---
            if was_assigned:
                # Confirm the reservation
                self.rc.state_manager.confirm_task_assignment(task.JobID)
                self.logger.info(f"Task {task.ID} successfully assigned to {worker_id}.", task_id=task.ID)
                # Dispatch to the correct queue
                if self.orchestrator.is_single_process_mode:
                    self.orchestrator._in_process_work_queue.put(task)
                else:
                    response_queue.put(task)
            else:
                # Release the reservation because the DB assignment failed (e.g., race condition)
                self.logger.warning(f"DB assignment failed for task {task.ID}. Releasing reservation.", task_id=task.ID)
                self.rc.release_task_reservation(task.DatasourceID)
                if not self.orchestrator.is_single_process_mode: 
                    self.orchestrator.waiting_workers.put((worker_id, response_queue))
                with self.orchestrator._state_lock: 
                    self.orchestrator.job_cache[task.JobID].appendleft(task)

        except Exception:
            self.logger.error(f"Critical error during dispatch for task {task.ID}. Releasing reservation.", task_id=task.ID, exc_info=True)
            self.rc.release_task_reservation(task.DatasourceID)