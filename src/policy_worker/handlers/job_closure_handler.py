"""
Handler for JOB_CLOSURE tasks.
Executes closure actions: timestamps, reporting, LATEST promotion, merge triggers.
"""

from typing import List
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity
from core.models.models import WorkPacket
from core.models.payloads import JobClosurePayload

from ..repositories.report_repository import ReportRepository

class JobClosureHandler:
    """
    Processes JOB_CLOSURE tasks by executing configured closure actions:
    - update_timestamps: Update datasource scan timestamps
    - execute_reporting: Generate policy reports
    - promote_to_latest: Copy job output to LATEST/MAIN or LATEST/DELTA
    - trigger_merge: Submit Flink batch job for LATEST/MERGED update
    - update_remediation: Update remediation lifecycle state
    - update_policy_checkpoint: Update incremental policy checkpoints
    """
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.db = policy_worker.db
        self.s3_promoter = policy_worker.s3_promoter
        self.merge_coordinator = policy_worker.merge_coordinator
        self.report_repo = ReportRepository(self.db, self.logger)
    
    async def process(self, work_packet: WorkPacket):
        """Execute job closure actions in sequence"""
        
        payload = JobClosurePayload.model_validate(work_packet.payload)
        trace_id = work_packet.header.trace_id
        
        self.logger.info(
            "Processing JOB_CLOSURE task",
            job_id=payload.completed_job_id,
            job_type=payload.job_type,
            closure_actions=payload.closure_actions,
            trace_id=trace_id
        )
        
        start_time = datetime.now()
        
        try:
            # Execute each closure action in order
            for action in payload.closure_actions:
                self.logger.info(
                    f"Executing closure action: {action}",
                    job_id=payload.completed_job_id,
                    action=action,
                    trace_id=trace_id
                )
                
                if action == "update_timestamps":
                    await self._update_datasource_timestamps(payload, trace_id)
                
                elif action == "execute_reporting":
                    await self._execute_reporting_policy(payload, trace_id)
                
                elif action == "promote_to_latest":
                    await self._promote_to_latest(payload, trace_id)
                
                elif action == "trigger_merge":
                    await self._trigger_merge(payload, trace_id)
                
                elif action == "update_remediation":
                    await self._update_remediation_lifecycle(payload, trace_id)
                
                elif action == "update_policy_checkpoint":
                    await self._update_policy_checkpoint(payload, trace_id)
                
                else:
                    self.logger.warning(
                        f"Unknown closure action: {action}",
                        job_id=payload.completed_job_id,
                        action=action,
                        trace_id=trace_id
                    )
            
            elapsed_seconds = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                "JOB_CLOSURE completed successfully",
                job_id=payload.completed_job_id,
                actions_executed=len(payload.closure_actions),
                execution_time_seconds=round(elapsed_seconds, 2),
                trace_id=trace_id
            )
        
        except Exception as e:
            elapsed_seconds = (datetime.now() - start_time).total_seconds()
            
            self.logger.error(
                "JOB_CLOSURE failed",
                job_id=payload.completed_job_id,
                execution_time_seconds=round(elapsed_seconds, 2),
                error=str(e),
                trace_id=trace_id
            )
            raise
    
    async def _update_datasource_timestamps(
        self,
        payload: JobClosurePayload,
        trace_id: str
    ):
        """Update last scan timestamps on datasource records"""
        
        timestamp_field = None
        
        if payload.job_type == "SCANNING":
            if payload.scan_mode == "full":
                timestamp_field = "last_classificationscantimestamp"
            elif payload.scan_mode == "delta":
                timestamp_field = "last_deltaclassificationscantimestamp"
        
        if not timestamp_field:
            self.logger.debug(
                "No timestamp update needed for this job type",
                job_id=payload.completed_job_id,
                job_type=payload.job_type,
                trace_id=trace_id
            )
            return
        
        # Update timestamps
        for datasource_id in payload.datasource_ids:
            await self.db.execute_raw_sql(f"""
                UPDATE datasources
                SET {timestamp_field} = SYSDATETIMEOFFSET()
                WHERE datasource_id = '{datasource_id}'
            """)
        
        self.logger.info(
            "Datasource timestamps updated",
            job_id=payload.completed_job_id,
            timestamp_field=timestamp_field,
            datasource_count=len(payload.datasource_ids),
            trace_id=trace_id
        )
    
    async def _execute_reporting_policy(
        self,
        payload: JobClosurePayload,
        trace_id: str
    ):
        """Generate reports if reporting policy template specified"""
        
        if not payload.reporting_policy_template_id:
            self.logger.debug(
                "No reporting policy specified, skipping report generation",
                job_id=payload.completed_job_id,
                trace_id=trace_id
            )
            return
        
        self.logger.info(
            "Generating policy reports",
            job_id=payload.completed_job_id,
            reporting_policy_id=payload.reporting_policy_template_id,
            trace_id=trace_id
        )
        
        # Get policy template
        policy_template = await self.db.get_policy_template(
            payload.reporting_policy_template_id
        )
        
        if not policy_template:
            raise ProcessingError(
                message=f"Reporting policy template not found: {payload.reporting_policy_template_id}",
                error_type=ErrorType.CONFIGURATION_MISSING,
                severity=ErrorSeverity.MEDIUM,
                context={"policy_template_id": payload.reporting_policy_template_id}
            )
        
        # Generate reports (implementation depends on reporting aggregation config)
        # This would involve building aggregation queries and writing results to S3
        
        # Placeholder for actual report generation logic
        report_paths = []  # List of generated report S3 paths
        
        # Record report metadata
        for report_path in report_paths:
            await self.report_repo.insert_report_data(
                job_id=payload.completed_job_id,
                policy_template_id=payload.reporting_policy_template_id,
                report_type="policy_aggregation",
                report_scope="datasource",
                s3_path=report_path,
                datasource_id=None,  # Would be set per datasource
                output_format="parquet"
            )
        
        self.logger.info(
            "Policy reports generated",
            job_id=payload.completed_job_id,
            reports_created=len(report_paths),
            trace_id=trace_id
        )
    
    async def _promote_to_latest(
        self,
        payload: JobClosurePayload,
        trace_id: str
    ):
        """Copy job output to LATEST/MAIN or LATEST/DELTA"""
        
        if not payload.write_to_latest:
            self.logger.debug(
                "write_to_latest=false, skipping promotion",
                job_id=payload.completed_job_id,
                trace_id=trace_id
            )
            return
        
        self.logger.info(
            "Promoting job output to LATEST",
            job_id=payload.completed_job_id,
            scan_mode=payload.scan_mode,
            datasource_count=len(payload.datasource_ids),
            trace_id=trace_id
        )
        
        for datasource_id in payload.datasource_ids:
            try:
                # Promote via S3Promoter service
                await self.s3_promoter.promote_job_output(
                    job_id=payload.completed_job_id,
                    datasource_id=datasource_id,
                    scan_mode=payload.scan_mode,
                    trace_id=trace_id
                )
                
                self.logger.info(
                    "Job output promoted successfully",
                    job_id=payload.completed_job_id,
                    datasource_id=datasource_id,
                    scan_mode=payload.scan_mode,
                    trace_id=trace_id
                )
            
            except Exception as e:
                self.logger.error(
                    "Failed to promote job output for datasource",
                    job_id=payload.completed_job_id,
                    datasource_id=datasource_id,
                    error=str(e),
                    trace_id=trace_id
                )
                # Continue with other datasources
    
    async def _trigger_merge(
        self,
        payload: JobClosurePayload,
        trace_id: str
    ):
        """Trigger Flink batch job to merge MAIN + DELTA → MERGED"""
        
        if not payload.write_to_latest:
            self.logger.debug(
                "write_to_latest=false, skipping merge",
                job_id=payload.completed_job_id,
                trace_id=trace_id
            )
            return
        
        self.logger.info(
            "Triggering S3 merge jobs",
            job_id=payload.completed_job_id,
            datasource_count=len(payload.datasource_ids),
            trace_id=trace_id
        )
        
        for datasource_id in payload.datasource_ids:
            try:
                # Trigger merge via MergeCoordinator
                merge_job_id = await self.merge_coordinator.trigger_merge(
                    scan_job_id=payload.completed_job_id,
                    datasource_id=datasource_id,
                    scan_type=payload.scan_mode,
                    trace_id=trace_id
                )
                
                self.logger.info(
                    "Merge job triggered successfully",
                    scan_job_id=payload.completed_job_id,
                    datasource_id=datasource_id,
                    merge_job_id=merge_job_id,
                    trace_id=trace_id
                )
            
            except Exception as e:
                self.logger.error(
                    "Failed to trigger merge job",
                    job_id=payload.completed_job_id,
                    datasource_id=datasource_id,
                    error=str(e),
                    trace_id=trace_id
                )
                # Continue with other datasources
    
    async def _update_remediation_lifecycle(
        self,
        payload: JobClosurePayload,
        trace_id: str
    ):
        """Update remediation lifecycle state (future implementation)"""
        
        self.logger.debug(
            "Remediation lifecycle update not yet implemented",
            job_id=payload.completed_job_id,
            trace_id=trace_id
        )
        # Placeholder for future implementation
    
    async def _update_policy_checkpoint(
        self,
        payload: JobClosurePayload,
        trace_id: str
    ):
        """Update incremental policy checkpoints (if policy job)"""
        
        if payload.job_type != "POLICY":
            self.logger.debug(
                "Not a policy job, skipping checkpoint update",
                job_id=payload.completed_job_id,
                job_type=payload.job_type,
                trace_id=trace_id
            )
            return
        
        # Checkpoint update would happen here for policy jobs
        # Similar to PolicyQueryHandler._update_incremental_checkpoints
        
        self.logger.debug(
            "Policy checkpoint update placeholder",
            job_id=payload.completed_job_id,
            trace_id=trace_id
        )
