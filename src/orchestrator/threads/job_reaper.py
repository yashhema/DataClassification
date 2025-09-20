# src/orchestrator/threads/job_reaper.py
"""
The JobReaper coroutine finds and recovers abandoned jobs where an
Orchestrator instance has failed or become unresponsive.
"""

import asyncio
from datetime import datetime, timezone, timedelta

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator
from core.errors import ErrorCategory
class JobReaper:
    """Coroutine to find and recover abandoned jobs."""

    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        # Get settings from the main orchestrator configuration
        self.interval_sec = orchestrator.settings.orchestrator.job_reaper_interval_seconds
        self.warning_threshold = 3  # Number of checks before takeover
        self.warning_window = timedelta(minutes=10) # Time window for warnings to be considered recent
        self.name = "JobReaperCoroutine"
        

    async def run_async(self):
        """The main async loop for the Job Reaper."""
        self.logger.log_component_lifecycle("JobReaper", "STARTED")

        while not self.orchestrator._shutdown_event.is_set():
            try:
                self.logger.info("JobReaper running check for abandoned jobs...")
                
                # Get the orchestrator's node group to only check for local jobs
                nodegroup = self.orchestrator.settings.system.node_group

                abandoned_jobs = await self.db.get_abandoned_jobs_for_nodegroup(nodegroup)

                if abandoned_jobs:
                    self.logger.warning(f"JobReaper found {len(abandoned_jobs)} abandoned jobs.", count=len(abandoned_jobs))
                    for job in abandoned_jobs:
                        await self._process_abandoned_job(job)

                self.orchestrator.update_thread_liveness("job_reaper")
                await asyncio.sleep(self.interval_sec)

            except Exception as e:
                
                error = self.orchestrator.error_handler.handle_error(e, "job_reaper_main_loop")
                
                # Fatal error detection and propagation
                if error.error_category == ErrorCategory.FATAL_BUG:
                    self.logger.critical(f"Fatal error detected in {self.__class__.__name__}: {error}")
                    raise  # Propagate to TaskManager
                
                # Existing retry logic for operational errors
                self.logger.warning(f"Transient error in {self.__class__.__name__}, retrying: {error}")
                

                await asyncio.sleep(self.interval_sec) # Avoid tight loop on failure

        self.logger.log_component_lifecycle("JobReaper", "STOPPED")

    async def _process_abandoned_job(self, job):
        """Applies the grace period logic and attempts takeover if necessary."""
        now = datetime.now(timezone.utc)
        
        # Check if previous warnings are stale
        is_stale = (job.last_lease_warning_timestamp is None or 
                    (now - job.last_lease_warning_timestamp) > self.warning_window)

        if is_stale:
            # First warning or previous warnings are too old; reset the count.
            await self.db.update_job_warning_count(job.id, new_count=1)
            self.logger.warning(f"Lease expired for job {job.id}. This is warning 1 of {self.warning_threshold}.", job_id=job.id)
        
        elif job.lease_warning_count < self.warning_threshold - 1:
            # Warnings are recent, but we are still within the grace period. Increment the count.
            new_count = job.lease_warning_count + 1
            await self.db.update_job_warning_count(job.id, new_count=new_count)
            self.logger.warning(f"Lease still expired for job {job.id}. This is warning {new_count} of {self.warning_threshold}.", job_id=job.id)

        else:
            # Grace period is over. Attempt to take over the job.
            self.logger.warning(f"Grace period exceeded for job {job.id}. Attempting takeover.", job_id=job.id)
            
            # The takeover method uses the 'Version' column for a safe, atomic update
            was_takeover_successful = await self.db.takeover_abandoned_job(
                job_id=job.id,
                job_version=job.version,
                new_orchestrator_id=self.orchestrator.instance_id
            )

            if was_takeover_successful:
                self.logger.info(f"Successfully took over abandoned job {job.id}. Failing the job and cleaning up tasks.", job_id=job.id)
                # Now that we own it, fail the job and cancel its pending tasks
                await self.db.fail_job(job.id, "Job failed due to orchestrator timeout and recovery.")
                await self.db.cancel_pending_tasks_for_job(job.id)
            else:
                self.logger.info(f"Takeover of job {job.id} failed. Another orchestrator may have recovered it or the original renewed its lease.", job_id=job.id)