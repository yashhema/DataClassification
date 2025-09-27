# src/orchestrator/threads/reaper.py
"""
The Reaper coroutine is a background process within the Orchestrator that
periodically scans for tasks that have exceeded their lease duration.
It is a critical component for system resilience, ensuring that hung
or failed workers do not cause tasks to be permanently stuck.

FIXES APPLIED:
- Fixed schema compliance: uses task.job_id instead of task.JobID
- Updated to call ResourceCoordinator directly instead of deleted orchestrator method
- Maintained all original timeout and recovery logic

ASYNC CONVERSION:
- Converted from threading.Thread to async coroutine
- All database calls now use await
- Uses asyncio.sleep instead of threading.Event.wait
"""

import asyncio

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator
from core.errors import ErrorCategory
class Reaper:
    """Coroutine responsible for finding and re-queuing hung or timed-out tasks."""
    
    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.interval = orchestrator.settings.orchestrator.reaper_interval_seconds
        self.task_timeout = orchestrator.settings.worker.task_timeout_seconds
        self.name = "ReaperCoroutine"

    async def run_async(self):
        """The main async loop for the Reaper coroutine."""
        self.logger.log_component_lifecycle("Reaper", "STARTED")
        
        while not self.orchestrator._shutdown_event.is_set():
            try:
                self.logger.info("Reaper running check for hung tasks...")
                
                # Fetch all tasks whose lease has expired
                self.logger.info(f"DEBUG: Reaper about to Query for expired task")
                expired_tasks = await self.db.get_expired_task_leases(self.task_timeout)
                
                if expired_tasks:
                    self.logger.warning("Reaper found  expired tasks.", count=len(expired_tasks))
                    for task in expired_tasks:
                        # FIXED: Use task.job_id instead of task.JobID
                        self.logger.log_lease_expiry(task.id, task.worker_id, job_id=task.job_id)
                        
                        # UPDATED: Call ResourceCoordinator directly instead of deleted orchestrator method
                        # Release worker slot for the job
                        self.orchestrator.resource_coordinator.state_manager.release_job_worker_slot(task.job_id)
                        
                        # Release datasource connection if the task had one
                        if task.datasource_id:
                            self.orchestrator.resource_coordinator.state_manager.release_datasource_connection(task.datasource_id)
                        
                        # Re-queue the task by marking it as failed and retryable.
                        await self.db.fail_task(task.id, is_retryable=True)
                else:
                    self.logger.warning("Reaper found 0 expired tasks.")
                # Update the liveness timestamp to show the coroutine is healthy and not deadlocked.
                self.orchestrator.update_thread_liveness("reaper")
                
                # Async sleep instead of threading.Event.wait
                await asyncio.sleep(self.interval)
            
            except Exception as e:
                # Catch exceptions to prevent the coroutine from crashing.
                error = self.orchestrator.error_handler.handle_error(e, "reaper_main_loop")
                # Avoid a tight loop on repeated database failures
                # Fatal error detection and propagation
                if error.error_category == ErrorCategory.FATAL_BUG:
                    self.logger.critical(f"Fatal error detected in {self.__class__.__name__}: {error}")
                    raise  # Propagate to TaskManager
                
                # Existing retry logic for operational errors
                self.logger.warning(f"Transient error in {self.__class__.__name__}, retrying: {error}")
                await asyncio.sleep(self.interval )
        self.logger.log_component_lifecycle("Reaper", "STOPPED")