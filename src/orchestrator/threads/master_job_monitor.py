# src/orchestrator/threads/master_job_monitor.py
import asyncio
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

from core.db_models.job_schema import JobStatus

if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class MasterJobMonitor:
    """
    Monitors Master Jobs for stuck transient states and final completion.
    This coroutine runs ONLY on the central manager Orchestrator instance.
    """
    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.interval_sec = 60  # Check every 60 seconds
        self.stuck_threshold_min = 60 # Alert if stuck for 1 hour

    async def run_async(self):
        """The main async loop for the Master Job Monitor."""
        self.logger.log_component_lifecycle("MasterJobMonitor", "STARTED")
        while not self.orchestrator._shutdown_event.is_set():
            try:
                active_master_jobs = await self.db.get_active_master_jobs_for_monitoring()
                
                for master_job in active_master_jobs:
                    await self._process_master_job(master_job)

                self.orchestrator.update_thread_liveness("master_job_monitor")
                await asyncio.sleep(self.interval_sec)
            except Exception as e:
                self.orchestrator.error_handler.handle_error(e, "master_job_monitor_main_loop")
                await asyncio.sleep(self.interval_sec * 2) # Longer backoff on error
        self.logger.log_component_lifecycle("MasterJobMonitor", "STOPPED")

    async def _process_master_job(self, master_job):
        """Processes a single active master job for potential state transitions."""
        context = {"master_job_id": master_job.master_job_id}
        summary = await self.db.get_master_job_summary(master_job.master_job_id, context)
        if not summary:
            self.logger.warning(f"Could not find summary record for active master job.", **context)
            return

        # Check for stuck transient states (circuit breaker)
        transient_states = [JobStatus.PAUSING, JobStatus.CANCELLING]
        if master_job.status in transient_states:
            # NOTE: last_updated is a proxy for the start of the stuck period. It reflects the last
            # time a child job changed state. The timer for a stuck job effectively begins when
            # all child activity has ceased but the master job's completion condition is not yet met.
            time_in_state = datetime.now(timezone.utc) - summary.last_updated
            if time_in_state > timedelta(minutes=self.stuck_threshold_min):
                self.logger.critical(
                    f"Master Job has been stuck in '{master_job.status.value}' state for over {self.stuck_threshold_min} minutes.",
                    **context
                )
        
        # CORRECTED: A job is considered ready for finalization when no children are in an active or transient state.
        # Queued children have not started, so they do not prevent completion checks.
        active_children = (summary.running_children + summary.pausing_children + 
                           summary.cancelling_children)

        # CORRECTED: This condition now correctly handles jobs that were created with zero children.
        if active_children == 0:
            await self._finalize_master_job(master_job, summary)

    async def _finalize_master_job(self, master_job, summary):
        """Performs just-in-time validation and sets the final status of a master job."""
        context = {"master_job_id": master_job.master_job_id}
        
        # NEW: Safety check to prevent re-finalizing an already completed job.
        terminal_states = [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.COMPLETED_WITH_FAILURES]
        if master_job.status in terminal_states:
            self.logger.debug(f"Master job {master_job.master_job_id} is already in a terminal state. Skipping finalization.", **context)
            return

        try:
            self.logger.info(f"All children for master job have completed. Performing final validation.", **context)

            # 1. Just-in-Time Validation: Recalculate to ensure 100% accuracy
            await self.db.recalculate_and_correct_summary(master_job.master_job_id, context)
            final_summary = await self.db.get_master_job_summary(master_job.master_job_id, context)
            
            if not final_summary:
                self.logger.error("Failed to retrieve final summary after recalculation. Aborting finalization.", **context)
                return

            # 2. Determine final status
            # DESIGN NOTE: The failure threshold is a top-level key in the master job's configuration JSON blob.
            failure_threshold = master_job.configuration.get("failure_threshold_percent", 100)
            failure_percent = (final_summary.failed_children / final_summary.total_children * 100) if final_summary.total_children > 0 else 0
            
            final_status = JobStatus.COMPLETED
            if failure_percent > failure_threshold:
                final_status = JobStatus.FAILED
            elif final_summary.failed_children > 0:
                final_status = JobStatus.COMPLETED_WITH_FAILURES
            
            # 3. Set final status
            await self.db.update_master_job_status(master_job.master_job_id, final_status, context)
            self.logger.info(f"Master job finalized with status '{final_status.value}'.", final_status=final_status.value, **context)
        
        except Exception as e:
            # NEW: Proper error handling for the finalization step.
            self.orchestrator.error_handler.handle_error(e, "finalize_master_job", **context)
            # Do not re-raise; allow the monitor to continue processing other jobs.

