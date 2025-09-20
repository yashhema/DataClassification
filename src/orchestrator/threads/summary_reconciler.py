import asyncio
from typing import TYPE_CHECKING, Dict

from core.db_models.job_schema import JobStatus

if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class SummaryReconciler:
    """
    Periodically validates and corrects counter drift in the master_job_state_summary table.
    This coroutine runs ONLY on the central manager Orchestrator instance as a failsafe.
    """

    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        # In a real system, this would be configurable
        self.interval_seconds = 3600  # Reconcile every hour

    async def run_async(self):
        """The main async loop for the Summary Reconciler coroutine."""
        self.logger.log_component_lifecycle("SummaryReconciler", "STARTED")
        
        # Initial delay to let the system stabilize after startup
        await asyncio.sleep(self.interval_seconds)

        while not self.orchestrator._shutdown_event.is_set():
            try:
                self.logger.info("SummaryReconciler starting periodic check for counter drift.")
                
                # Get all master jobs that are in a non-terminal state
                active_master_jobs = await self.db.get_active_master_jobs_for_monitoring()

                for master_job in active_master_jobs:
                    try:
                        await self._reconcile_single_master_job(master_job)
                    except Exception as job_error:
                        self.orchestrator.error_handler.handle_error(
                            job_error, 
                            "reconcile_single_master_job", 
                            master_job_id=master_job.master_job_id
                        )
                        # Continue to the next job even if one fails
                        continue
                
                self.logger.info("SummaryReconciler check completed.")
                self.orchestrator.update_thread_liveness("summary_reconciler")

            except Exception as e:
                self.orchestrator.error_handler.handle_error(e, "summary_reconciler_main_loop")

            await asyncio.sleep(self.interval_seconds)

        self.logger.log_component_lifecycle("SummaryReconciler", "STOPPED")

    async def _reconcile_single_master_job(self, master_job):
        """Performs a full recount for a single master job and corrects drift."""
        master_job_id = master_job.master_job_id
        context = {"master_job_id": master_job_id}

        # 1. Get the true counts by querying the child jobs table
        true_counts = await self.db.get_child_job_counts_by_status(master_job_id, context=context)
        
        # 2. Get the currently stored summary counts
        summary = await self.db.get_master_job_summary(master_job_id, context=context)

        if not summary:
            self.logger.warning(f"Cannot reconcile master job '{master_job_id}': summary record not found.", **context)
            return

        # 3. Compare and find any drift
        drift_detected = False
        updates: Dict[str, int] = {}
        
        status_map = {
            JobStatus.QUEUED: summary.queued_children,
            JobStatus.RUNNING: summary.running_children,
            JobStatus.PAUSING: summary.pausing_children,
            JobStatus.PAUSED: summary.paused_children,
            JobStatus.CANCELLING: summary.cancelling_children,
            JobStatus.CANCELLED: summary.cancelled_children,
            JobStatus.COMPLETED: summary.completed_children,
            JobStatus.FAILED: summary.failed_children
        }

        for status, stored_count in status_map.items():
            true_count = true_counts.get(status, 0)
            if stored_count != true_count:
                drift_detected = True
                updates[f"{status.value.lower()}_children"] = true_count
        
        # 4. If drift is found, log it and correct the record
        if drift_detected:
            self.logger.warning(
                f"Counter drift detected for master job '{master_job_id}'. Correcting record.",
                current_summary=status_map,
                true_counts=true_counts,
                **context
            )
            await self.db.recalculate_and_correct_summary(master_job_id, updates, context=context)
        else:
            self.logger.info(f"Counters for master job '{master_job_id}' are consistent. No drift detected.", **context)

