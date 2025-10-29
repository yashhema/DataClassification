"""
S3 promotion service for moving job output to LATEST branch.
Handles MAIN/DELTA paths and archive operations.
"""

from datetime import datetime
from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity

class S3Promoter:
    """
    Promotes job output to LATEST branch:
    - Full scans → LATEST/MAIN/{job_id}/
    - Delta scans → LATEST/DELTA/{date}/{job_id}/
    - Archives old MERGED state before full scan promotion
    """
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.s3_client = policy_worker.s3_client
        self.s3_config = policy_worker.config.s3_config
    
    async def promote_job_output(
        self,
        job_id: int,
        datasource_id: str,
        scan_mode: str,
        trace_id: str
    ):
        """
        Promote job output to LATEST branch with appropriate structure.
        
        Full scan:
            1. Archive current MERGED
            2. Clear old MAIN
            3. Copy job output to LATEST/MAIN/{job_id}/
        
        Delta scan:
            1. Copy job output to LATEST/DELTA/{date}/{job_id}/
        """
        
        self.logger.info(
            "Starting job output promotion",
            job_id=job_id,
            datasource_id=datasource_id,
            scan_mode=scan_mode,
            trace_id=trace_id
        )
        
        bucket = self.s3_config.bucket_name
        
        # Source: job-specific output
        source_base = f"s3://{bucket}/scan_data/job_specific_output/job_id={job_id}/datasource_id={datasource_id}/"
        
        try:
            if scan_mode == "full":
                await self._promote_full_scan(
                    job_id=job_id,
                    datasource_id=datasource_id,
                    source_base=source_base,
                    trace_id=trace_id
                )
            else:  # delta
                await self._promote_delta_scan(
                    job_id=job_id,
                    datasource_id=datasource_id,
                    source_base=source_base,
                    trace_id=trace_id
                )
            
            self.logger.info(
                "Job output promoted successfully",
                job_id=job_id,
                datasource_id=datasource_id,
                scan_mode=scan_mode,
                trace_id=trace_id
            )
        
        except Exception as e:
            raise ProcessingError(
                message=f"Failed to promote job output: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.HIGH,
                context={
                    "job_id": job_id,
                    "datasource_id": datasource_id,
                    "scan_mode": scan_mode,
                    "trace_id": trace_id
                },
                cause=e
            )
    
    async def _promote_full_scan(
        self,
        job_id: int,
        datasource_id: str,
        source_base: str,
        trace_id: str
    ):
        """Promote full scan to LATEST/MAIN"""
        
        bucket = self.s3_config.bucket_name
        
        # Step 1: Archive current MERGED to history
        self.logger.debug(
            "Archiving current MERGED state",
            datasource_id=datasource_id,
            trace_id=trace_id
        )
        
        await self._archive_merged_to_history(datasource_id)
        
        # Step 2: Clear old MAIN (if exists)
        self.logger.debug(
            "Clearing old MAIN data",
            datasource_id=datasource_id,
            trace_id=trace_id
        )
        
        old_main_base = f"s3://{bucket}/scan_data/LATEST/MAIN/"
        await self.s3_client.delete_recursive(old_main_base)
        
        # Step 3: Copy job output to LATEST/MAIN
        target_base = f"s3://{bucket}/scan_data/LATEST/MAIN/{job_id}/datasource_id={datasource_id}/"
        
        self.logger.debug(
            "Copying job output to LATEST/MAIN",
            source=source_base,
            target=target_base,
            trace_id=trace_id
        )
        
        await self.s3_client.copy_recursive(source_base, target_base)
        
        # Step 4: Optionally clear old DELTA (new baseline)
        # Commenting out for safety - can be enabled if desired
        # delta_base = f"s3://{bucket}/scan_data/LATEST/DELTA/"
        # await self.s3_client.delete_recursive(delta_base)
        
        self.logger.info(
            "Full scan promoted to LATEST/MAIN",
            job_id=job_id,
            datasource_id=datasource_id,
            target=target_base,
            trace_id=trace_id
        )
    
    async def _promote_delta_scan(
        self,
        job_id: int,
        datasource_id: str,
        source_base: str,
        trace_id: str
    ):
        """Promote delta scan to LATEST/DELTA"""
        
        bucket = self.s3_config.bucket_name
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Copy to LATEST/DELTA/{date}/{job_id}/
        target_base = f"s3://{bucket}/scan_data/LATEST/DELTA/{today}/{job_id}/datasource_id={datasource_id}/"
        
        self.logger.debug(
            "Copying job output to LATEST/DELTA",
            source=source_base,
            target=target_base,
            trace_id=trace_id
        )
        
        await self.s3_client.copy_recursive(source_base, target_base)
        
        self.logger.info(
            "Delta scan promoted to LATEST/DELTA",
            job_id=job_id,
            datasource_id=datasource_id,
            target=target_base,
            trace_id=trace_id
        )
    
    async def _archive_merged_to_history(self, datasource_id: str):
        """Archive current MERGED state to history branch"""
        
        bucket = self.s3_config.bucket_name
        
        # Check if MERGED exists
        merged_path = f"s3://{bucket}/scan_data/LATEST/MERGED/datasource_id={datasource_id}/"
        exists = await self.s3_client.path_exists(merged_path)
        
        if not exists:
            self.logger.debug(
                "No MERGED state to archive",
                datasource_id=datasource_id
            )
            return
        
        # Archive to history with current date
        today = datetime.now().strftime("%Y-%m-%d")
        archive_path = f"s3://{bucket}/scan_data/history/date={today}/MERGED/datasource_id={datasource_id}/"
        
        self.logger.info(
            "Archiving MERGED to history",
            source=merged_path,
            target=archive_path,
            datasource_id=datasource_id
        )
        
        await self.s3_client.copy_recursive(merged_path, archive_path)
        
        # Optionally delete old MERGED (will be recreated by merge job)
        # await self.s3_client.delete_recursive(merged_path)
