# src/orchestrator/threads/reaper.py
"""
The Reaper coroutine is a background process within the Orchestrator that
periodically scans for tasks that have exceeded their lease duration.
It is a critical component for system resilience, ensuring that hung
or failed workers do not cause tasks to be permanently stuck.

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
                expired_tasks = await self.db.get_expired_task_leases(self.task_timeout)
                
                if expired_tasks:
                    self.logger.warning(f"Reaper found {len(expired_tasks)} expired tasks.", count=len(expired_tasks))
                    for task in expired_tasks:
                        self.logger.log_lease_expiry(task.ID, task.WorkerID, job_id=task.JobID)
                        
                        # Use the central Orchestrator method to release resources,
                        # ensuring consistent state management.
                        self.orchestrator.release_resources_for_task(task)
                        
                        # Re-queue the task by marking it as failed and retryable.
                        await self.db.fail_task(task.ID, is_retryable=True)
                
                # Update the liveness timestamp to show the coroutine is healthy and not deadlocked.
                self.orchestrator.update_thread_liveness("reaper")
                
                # Async sleep instead of threading.Event.wait
                await asyncio.sleep(self.interval)
            
            except Exception as e:
                # Catch exceptions to prevent the coroutine from crashing.
                self.logger.error("An unexpected error occurred in the Reaper loop.", exc_info=True)
                # Avoid a tight loop on repeated database failures
                await asyncio.sleep(self.interval)

        self.logger.log_component_lifecycle("Reaper", "STOPPED")