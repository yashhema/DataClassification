# src/orchestrator/threads/job_monitor.py
"""
The JobCompletionMonitor is a background coroutine that monitors the status of
active jobs. It is responsible for transitioning jobs to their final state
(e.g., COMPLETED, FAILED) once all their tasks are finished or a failure
threshold is met. It also handles the graceful transition for paused jobs.

FIXES APPLIED:
- Added JobState enum import for finalize_job_state calls
- Updated finalize_job_state method calls to use JobState enums instead of strings
- Maintained correct JobStatus enum usage for database queries
ASYNC CONVERSION:
- Converted from threading.Thread to async coroutine
- All database calls now use await
- Uses asyncio.sleep instead of threading.Event.wait
"""

import asyncio
from core.db_models.job_schema import JobStatus
from core.config.configuration_manager import ClassificationConfidenceConfig
# Import for type hinting and JobState enum access
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator
# UPDATED: Import JobState from the new, neutral file
from orchestrator.orchestrator_state import JobState

class JobCompletionMonitor:
    """
    Monitors running jobs for completion, failure thresholds, and handles
    the final state transition for PAUSING jobs.
    """
    
    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.interval = orchestrator.settings.orchestrator.job_monitor_interval_sec
        self.name = "JobMonitorCoroutine"

    async def run_async(self):
        """The main async loop for the Job Monitor coroutine."""
        self.logger.log_component_lifecycle("JobMonitor", "STARTED")
        
        while not self.orchestrator._shutdown_event.is_set():
            try:
                # Fetch all jobs that are in a non-terminal state
                # Using JobStatus enums (correct - matches database interface expectation)
                jobs_to_check = await self.db.get_active_jobs(
                    statuses=[JobStatus.RUNNING, JobStatus.PAUSING]
                )
                
                for job in jobs_to_check:
                    try:
                        await self._process_job(job)
                    except Exception as job_error:
                        self.logger.error(
                            f"Error processing job {job.id} in JobMonitor",
                            job_id=job.id,
                            exc_info=True
                        )
                        # Continue processing other jobs even if one fails
                        continue
                
                # Update liveness timestamp after a successful loop
                self.orchestrator.update_thread_liveness("job_monitor")
                
                # Async sleep instead of threading.Event.wait
                await asyncio.sleep(self.interval)
                
            except Exception as e:
                self.orchestrator.error_handler.handle_error(
                    e, 
                    context="job_monitor_main_loop",
                    operation="main_loop_iteration"
                )                # Avoid tight loop on repeated database failures
                await asyncio.sleep(self.interval)
        
        self.logger.log_component_lifecycle("JobMonitor", "STOPPED")

    async def _process_job(self, job):
        """Process a single job for state transitions."""
        stats = await self.db.get_job_progress_summary(job.id)
        total_tasks = sum(stats.values())
        assigned_tasks = stats.get("ASSIGNED", 0)
        
        # --- Handle PAUSING -> PAUSED transition ---
        # This state is entered when a user requests a pause. The job becomes
        # fully PAUSED only after all its assigned tasks have finished.
        if job.status == JobStatus.PAUSING:
            if assigned_tasks == 0:
                # Atomically transition the state in the DB
                if await self.db.transition_job_from_pausing_to_paused(job.id):
                    # FIXED: Use JobState enum instead of string
                    self.orchestrator.finalize_job_state(job.id, JobState.PAUSED)
            return  # Don't check other transitions for pausing jobs

        # --- Handle RUNNING -> COMPLETED transition ---
        if job.status == JobStatus.RUNNING:
            pending_tasks = stats.get("PENDING", 0)
            
            # Job is complete when there are tasks and none are pending or assigned
            if total_tasks > 0 and pending_tasks == 0 and assigned_tasks == 0:
                # FIXED: Use JobState enum instead of string
                self.orchestrator.finalize_job_state(job.id, JobState.COMPLETED)
                return

            # --- Handle RUNNING -> FAILED transition ---
            failed_tasks = stats.get("FAILED", 0)
            
            # Check failure threshold if we have scan template configuration
            if job.scan_template and total_tasks > 0:  # Added total_tasks > 0 check for safety
                failure_threshold = job.scan_template.configuration.get("failure_threshold_percent", 10)
                
                # Check threshold only after a meaningful number of tasks have run
                # The total_tasks > 10 check also prevents division by zero
                if total_tasks > 10 and (failed_tasks / total_tasks * 100) >= failure_threshold:
                    self.logger.warning(
                        f"Job {job.id} breached failure threshold: {failed_tasks}/{total_tasks} tasks failed ({failed_tasks/total_tasks*100:.1f}% >= {failure_threshold}%)",
                        job_id=job.id,
                        failed_tasks=failed_tasks,
                        total_tasks=total_tasks,
                        failure_threshold=failure_threshold
                    )
                    # FIXED: Use JobState enum instead of string
                    self.orchestrator.finalize_job_state(job.id, JobState.FAILED)
                    return

        # --- Handle CANCELLING -> CANCELLED transition ---
        if job.status == JobStatus.CANCELLING:
            # Job is fully cancelled when no tasks are assigned (pending tasks already cancelled)
            if assigned_tasks == 0:
                # FIXED: Use JobState enum instead of string  
                self.orchestrator.finalize_job_state(job.id, JobState.CANCELLED)