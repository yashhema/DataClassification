# src/orchestrator/threads/job_monitor.py
"""
The JobCompletionMonitor is a background coroutine that monitors the status of
active jobs. It is responsible for transitioning jobs to their final state
(e.g., COMPLETED, FAILED) once all their tasks are finished or a failure
threshold is met. It also handles the graceful transition for paused jobs and
drives the event-driven updates to the master job summary table.
"""

import asyncio
import random
from typing import TYPE_CHECKING
from core.db_models.job_schema import JobStatus
from datetime import datetime, timezone, timedelta
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

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
                jobs_to_check = await self.db.get_active_jobs(
                    statuses=[JobStatus.RUNNING, JobStatus.PAUSING, JobStatus.CANCELLING]
                )
                
                for job in jobs_to_check:
                    try:
                        await self._process_job(job)
                    except Exception as job_error:
                        self.orchestrator.error_handler.handle_error(
                            job_error, 
                            "job_monitor_process_job",
                            job_id=job.id
                        )
                        continue
                
                self.orchestrator.update_thread_liveness("job_monitor")
                await asyncio.sleep(self.interval)
                
            except Exception as e:
                self.orchestrator.error_handler.handle_error(
                    e, 
                    "job_monitor_main_loop"
                )
                await asyncio.sleep(self.interval)
        
        self.logger.log_component_lifecycle("JobMonitor", "STOPPED")

    async def _process_job(self, job):
        """
        Processes a single child job for state transitions, including completion,
        failure, and pause states.
        """
        stats = await self.db.get_job_progress_summary(job.id) 
        total_tasks = sum(stats.values())
        assigned_tasks = stats.get('ASSIGNED', 0)
        pending_tasks = stats.get('PENDING', 0)
        failed_tasks = stats.get('FAILED', 0)
        # --- NEW: Logic to detect and RE-QUEUE orphaned jobs ---
        if total_tasks == 0 and job.status == JobStatus.RUNNING:
            time_since_creation = datetime.now(timezone.utc) - job.created_timestamp
            
            if time_since_creation > timedelta(minutes=5):
                self.logger.warning(
                    f"Job {job.id} has been in RUNNING state for over 5 minutes with zero tasks. Resetting to QUEUED for recovery.",
                    job_id=job.id
                )
                # This is a safe, self-healing action.
                await self.db.requeue_job(job.id) # A new, dedicated DB method for this is cleanest
                return
        # Handle PAUSING -> PAUSED transition
        if job.status == JobStatus.PAUSING and assigned_tasks == 0:
            self.logger.info(f"Child Job {job.id} has no more active tasks. Transitioning to PAUSED.", job_id=job.id)
            await self.db.update_job_status(job.id, JobStatus.PAUSED)
            await self._update_master_summary_with_retry(job.master_job_id, JobStatus.PAUSING, JobStatus.PAUSED)
            return

        # Infallible Completion Check
        is_task_work_done = total_tasks > 0 and pending_tasks == 0 and assigned_tasks == 0
        has_pending_outputs = await self.db.has_pending_output_records(job.id)
        is_job_fully_complete = is_task_work_done and not has_pending_outputs
        if all_tasks_complete:
            self.logger.info(
                "All tasks complete for job, creating closure task",
                job_id=job.id
            )
            # NEW: Create JOB_CLOSURE task
            await self._create_closure_task(job)
        

        if is_job_fully_complete:
            final_status = JobStatus.COMPLETED if failed_tasks == 0 else JobStatus.COMPLETED_WITH_FAILURES
            self.logger.info(f"Child Job {job.id} has completed all work. Final status: {final_status.value}", job_id=job.id)
            await self.db.update_job_status(job.id, final_status)
            await self._update_master_summary_with_retry(job.master_job_id, job.status, final_status)
            return

        # Check for failure threshold breach
        if total_tasks > 10: # Check only after a meaningful number of tasks
            master_job = await self.db.get_master_job_by_id_with_cache(job.master_job_id)
            if not master_job:
                self.logger.warning(f"Could not find master job for child job {job.id} to check failure threshold.", job_id=job.id)
                return
            
            failure_threshold = master_job.configuration.get("failure_threshold_percent", 10)
            failure_percent = (failed_tasks / total_tasks * 100)
            if failure_percent >= failure_threshold:
                self.logger.warning(
                    f"Child Job {job.id} breached failure threshold.",
                    job_id=job.id,
                    failed_tasks=failed_tasks,
                    total_tasks=total_tasks,
                    failure_threshold=failure_threshold
                )
                await self.db.update_job_status(job.id, JobStatus.FAILED)
                await self._update_master_summary_with_retry(job.master_job_id, job.status, JobStatus.FAILED)
                return


    async def _create_closure_task(self, job: Job):
        """
        Create JOB_CLOSURE task for completed job.
        Determines closure actions based on job template configuration.
        """
        import uuid
        from datetime import datetime, timezone
        
        # Get job template (ScanTemplate or PolicyTemplate)
        template = await self._get_job_template(job)
        
        if not template:
            self.logger.warning(
                "No template found for job, skipping closure",
                job_id=job.id
            )
            return
        
        # Determine closure actions based on template config
        closure_actions = []
        
        if job.job_type == "SCANNING":
            closure_actions.append("update_timestamps")
            
            # Check if write_to_latest enabled
            if template.write_to_latest:
                closure_actions.append("promote_to_latest")
                closure_actions.append("trigger_merge")
            
            # Check if reporting policy configured
            if template.reporting_policy_template_id:
                closure_actions.append("execute_reporting")
        
        elif job.job_type == "POLICY":
            closure_actions.append("update_policy_checkpoint")
            
            if template.policy_type == "remediationlifecycle":
                closure_actions.append("update_remediation")
        
        # Get datasource IDs from job tasks
        datasource_ids = await self._get_job_datasource_ids(job.id)
        
        # Create JOB_CLOSURE task
        task_id = uuid.uuid4().bytes
        
        work_packet = {
            "header": {
                "task_id": task_id.hex(),
                "job_id": job.id,
                "trace_id": f"trace_{uuid.uuid4().hex}",
                "created_timestamp": datetime.now(timezone.utc).isoformat()
            },
            "config": {
                "batch_write_size": 1000,
                "retry_count": 0
            },
            "payload": {
                "task_type": "JOB_CLOSURE",
                "completed_job_id": job.id,
                "job_type": job.job_type,
                "datasource_ids": datasource_ids,
                "template_id": job.template_id,
                "closure_actions": closure_actions,
                "write_to_latest": template.write_to_latest if hasattr(template, 'write_to_latest') else False,
                "scan_mode": template.scan_mode if hasattr(template, 'scan_mode') else None,
                "reporting_policy_template_id": template.reporting_policy_template_id if hasattr(template, 'reporting_policy_template_id') else None
            }
        }
        
        # Insert task directly (not via Pipeliner)
        await self.db.insert_task(
            task_id=task_id,
            job_id=job.id,
            task_type="JOB_CLOSURE",
            status="PENDING",
            eligible_worker_type="POLICY",  # Route to PolicyWorker
            node_group="central",
            work_packet=work_packet
        )
        
        self.logger.info(
            "JOB_CLOSURE task created",
            job_id=job.id,
            task_id=task_id.hex(),
            closure_actions=closure_actions
        )

    async def _get_job_template(self, job: Job):
        """Get ScanTemplate or PolicyTemplate for job"""
        
        if job.job_type == "SCANNING":
            return await self.db.get_scan_template(job.template_id)
        elif job.job_type == "POLICY":
            return await self.db.get_policy_template(job.template_id)
        
        return None

    async def _get_job_datasource_ids(self, job_id: int) -> List[str]:
        """Get unique datasource IDs from job tasks"""
        
        result = await self.db.execute_raw_sql(f"""
            SELECT DISTINCT datasource_id
            FROM tasks
            WHERE job_id = {job_id}
              AND datasource_id IS NOT NULL
            ORDER BY datasource_id
        """)
        
        return [row["datasource_id"] for row in result]


    
    async def _update_master_summary_with_retry(self, master_job_id: str, from_status: JobStatus, to_status: JobStatus, max_retries: int = 5):
        """
        Atomically updates the master job summary counters with a retry loop
        to handle optimistic locking failures.
        """
        if not master_job_id:
            self.logger.debug("Skipping summary update for child job with no master_job_id.")
            return

        retry_count = 0
        from_status_val = from_status.value if from_status else 'UNKNOWN'
        to_status_val = to_status.value if to_status else 'UNKNOWN'
        context = {"master_job_id": master_job_id, "from_status": from_status_val, "to_status": to_status_val}
        
        while retry_count < max_retries:
            try:
                summary = await self.db.get_master_job_summary(master_job_id, context)
                if not summary:
                    self.logger.error(f"Cannot update counters for missing summary record.", **context)
                    return

                was_successful = await self.db.update_master_job_summary_counters(
                    master_job_id=master_job_id,
                    from_status=from_status,
                    to_status=to_status,
                    current_version=summary.version,
                    context=context
                )
                if was_successful:
                    self.logger.info(f"Successfully updated master job summary counters.", **context)
                    return

                self.logger.warning(f"Optimistic lock failure updating summary. Retrying...", attempt=retry_count + 1, **context)

            except Exception as e:
                self.orchestrator.error_handler.handle_error(e, "_update_master_summary_with_retry", **context)
                break 

            # Exponential backoff with jitter
            delay = (2 ** retry_count) * 0.1 + (random.uniform(0, 0.1))
            await asyncio.sleep(delay)
            retry_count += 1
        
        self.logger.error(f"Failed to update master job summary after {max_retries} retries. Enqueuing for reconciliation.", **context)
        # Proactively trigger reconciliation for this specific master job
        if self.orchestrator.summary_reconciler:
            await self.orchestrator.summary_reconciler.enqueue_reconciliation_request(master_job_id)

