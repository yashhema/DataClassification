"""
Coordinates S3 merge batch jobs via Flink API.
Triggers merge jobs and monitors completion.
"""

import asyncio
import aiohttp
from typing import Optional
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import NetworkError, ProcessingError, ErrorType, ErrorSeverity

class MergeCoordinator:
    """
    Coordinates S3 merge batch jobs:
    1. Determine paths to merge (MAIN + DELTA)
    2. Submit Flink batch job via REST API
    3. Monitor job completion
    4. Update merge job tracking table
    """
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.db = policy_worker.db
        self.config = policy_worker.config.flink_config
        self.s3_config = policy_worker.config.s3_config
    
    async def trigger_merge(
        self,
        scan_job_id: int,
        datasource_id: str,
        scan_type: str,
        trace_id: str
    ) -> str:
        """
        Trigger Flink batch merge job.
        
        Returns: merge_job_id
        """
        
        merge_job_id = f"merge_{scan_job_id}_{datasource_id}_{int(datetime.now().timestamp())}"
        
        self.logger.info(
            "Starting S3 merge job trigger",
            merge_job_id=merge_job_id,
            scan_job_id=scan_job_id,
            datasource_id=datasource_id,
            scan_type=scan_type,
            trace_id=trace_id
        )
        
        try:
            # Get paths to merge
            main_path = await self._get_latest_main_path(datasource_id)
            delta_paths = await self._get_all_delta_paths(datasource_id)
            output_path = f"s3://{self.s3_config.bucket_name}/scan_data/LATEST/MERGED/datasource_id={datasource_id}/"
            
            self.logger.debug(
                "Merge paths identified",
                merge_job_id=merge_job_id,
                main_path=main_path,
                delta_path_count=len(delta_paths),
                output_path=output_path,
                trace_id=trace_id
            )
            
            # Create merge job record
            await self._create_merge_job_record(
                merge_job_id=merge_job_id,
                scan_job_id=scan_job_id,
                datasource_id=datasource_id,
                scan_type=scan_type,
                main_path=main_path,
                output_path=output_path
            )
            
            # Submit Flink batch job
            flink_job_id = await self._submit_flink_job(
                merge_job_id=merge_job_id,
                main_path=main_path,
                delta_paths=delta_paths,
                output_path=output_path,
                trace_id=trace_id
            )
            
            # Update merge job with Flink job ID
            await self.db.execute_raw_sql(f"""
                UPDATE s3_merge_jobs
                SET merge_batch_job_id = '{flink_job_id}',
                    status = 'running',
                    started_at = SYSDATETIMEOFFSET()
                WHERE merge_job_id = '{merge_job_id}'
            """)
            
            self.logger.info(
                "Flink merge job submitted successfully",
                merge_job_id=merge_job_id,
                flink_job_id=flink_job_id,
                trace_id=trace_id
            )
            
            # Start monitoring task (non-blocking)
            asyncio.create_task(
                self._monitor_merge_completion(
                    merge_job_id=merge_job_id,
                    flink_job_id=flink_job_id,
                    trace_id=trace_id
                )
            )
            
            return merge_job_id
        
        except Exception as e:
            self.logger.error(
                "Failed to trigger merge job",
                merge_job_id=merge_job_id,
                error=str(e),
                trace_id=trace_id
            )
            
            # Update merge job status
            try:
                await self.db.execute_raw_sql(f"""
                    UPDATE s3_merge_jobs
                    SET status = 'failed',
                        error_message = '{str(e).replace("'", "''")}'
                    WHERE merge_job_id = '{merge_job_id}'
                """)
            except:
                pass
            
            raise ProcessingError(
                message=f"Failed to trigger S3 merge: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.HIGH,
                context={
                    "merge_job_id": merge_job_id,
                    "datasource_id": datasource_id,
                    "trace_id": trace_id
                },
                cause=e
            )
    
    async def _get_latest_main_path(self, datasource_id: str) -> str:
        """Get path to latest MAIN scan for datasource"""
        
        # List MAIN directory to find latest job
        main_base = f"s3://{self.s3_config.bucket_name}/scan_data/LATEST/MAIN/"
        
        # Use S3 client to list directories
        # Assuming s3_client has list_prefixes method
        job_dirs = await self.policy_worker.s3_client.list_prefixes(main_base)
        
        if not job_dirs:
            raise ProcessingError(
                message=f"No MAIN path found for datasource {datasource_id}",
                error_type=ErrorType.PROCESSING_RESOURCE_NOT_FOUND,
                severity=ErrorSeverity.MEDIUM,
                context={"datasource_id": datasource_id}
            )
        
        # Get the latest (should only be one after cleanup)
        latest_job_dir = sorted(job_dirs)[-1]
        main_path = f"{main_base}{latest_job_dir}/datasource_id={datasource_id}/"
        
        return main_path
    
    async def _get_all_delta_paths(self, datasource_id: str) -> list:
        """Get all DELTA paths for datasource"""
        
        delta_base = f"s3://{self.s3_config.bucket_name}/scan_data/LATEST/DELTA/"
        
        # List date directories
        date_dirs = await self.policy_worker.s3_client.list_prefixes(delta_base)
        
        delta_paths = []
        for date_dir in date_dirs:
            # List job directories under each date
            date_path = f"{delta_base}{date_dir}/"
            job_dirs = await self.policy_worker.s3_client.list_prefixes(date_path)
            
            for job_dir in job_dirs:
                delta_path = f"{date_path}{job_dir}/datasource_id={datasource_id}/"
                
                # Check if path exists
                exists = await self.policy_worker.s3_client.path_exists(delta_path)
                if exists:
                    delta_paths.append(delta_path)
        
        return delta_paths
    
    async def _create_merge_job_record(
        self,
        merge_job_id: str,
        scan_job_id: int,
        datasource_id: str,
        scan_type: str,
        main_path: str,
        output_path: str
    ):
        """Create tracking record for merge job"""
        
        await self.db.execute_raw_sql(f"""
            INSERT INTO s3_merge_jobs (
                merge_job_id, scan_job_id, datasource_id, scan_type,
                status, source_path, target_path, created_at
            )
            VALUES (
                '{merge_job_id}',
                {scan_job_id},
                '{datasource_id}',
                '{scan_type}',
                'pending',
                '{main_path}',
                '{output_path}',
                SYSDATETIMEOFFSET()
            )
        """)
    
    async def _submit_flink_job(
        self,
        merge_job_id: str,
        main_path: str,
        delta_paths: list,
        output_path: str,
        trace_id: str
    ) -> str:
        """Submit Flink batch job via REST API"""
        
        # Build Flink job submission payload
        job_args = [
            "--merge-job-id", merge_job_id,
            "--main-path", main_path,
            "--delta-paths", ",".join(delta_paths) if delta_paths else "",
            "--output-path", output_path,
            "--parallelism", str(self.config.parallelism)
        ]
        
        payload = {
            "entryClass": "com.company.flink.batch.S3MergeJob",
            "programArgs": " ".join(job_args),
            "parallelism": self.config.parallelism,
            "savepointPath": None,
            "allowNonRestoredState": False
        }
        
        # Submit via Flink REST API
        url = f"{self.config.jobmanager_url}/jars/{self._get_jar_id()}/run"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=30) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise NetworkError(
                            message=f"Flink API returned {response.status}: {error_text}",
                            error_type=ErrorType.NETWORK_PROTOCOL_ERROR,
                            severity=ErrorSeverity.HIGH,
                            context={"url": url, "status": response.status}
                        )
                    
                    result = await response.json()
                    flink_job_id = result.get("jobid")
                    
                    if not flink_job_id:
                        raise ProcessingError(
                            message="Flink API did not return job ID",
                            error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                            severity=ErrorSeverity.HIGH,
                            context={"response": result}
                        )
                    
                    self.logger.info(
                        "Flink batch job submitted",
                        merge_job_id=merge_job_id,
                        flink_job_id=flink_job_id,
                        trace_id=trace_id
                    )
                    
                    return flink_job_id
        
        except aiohttp.ClientError as e:
            raise NetworkError(
                message=f"Failed to connect to Flink API: {str(e)}",
                error_type=ErrorType.NETWORK_CONNECTION_FAILED,
                severity=ErrorSeverity.HIGH,
                retryable=True,
                context={"url": url},
                cause=e
            )
    
    def _get_jar_id(self) -> str:
        """Get JAR ID from Flink (simplified - assumes JAR already uploaded)"""
        # In production, would query /jars endpoint to find uploaded JAR
        # For now, return placeholder
        return "merge-job-jar"
    
    async def _monitor_merge_completion(
        self,
        merge_job_id: str,
        flink_job_id: str,
        trace_id: str
    ):
        """Monitor Flink job status until completion (background task)"""
        
        self.logger.info(
            "Starting merge job monitoring",
            merge_job_id=merge_job_id,
            flink_job_id=flink_job_id,
            trace_id=trace_id
        )
        
        poll_interval = 30  # seconds
        max_duration = 3600 * 4  # 4 hours
        elapsed = 0
        
        try:
            while elapsed < max_duration:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval
                
                # Query Flink job status
                status = await self._get_flink_job_status(flink_job_id)
                
                self.logger.debug(
                    "Merge job status check",
                    merge_job_id=merge_job_id,
                    flink_job_id=flink_job_id,
                    status=status,
                    elapsed_seconds=elapsed,
                    trace_id=trace_id
                )
                
                if status == "FINISHED":
                    await self._handle_merge_success(merge_job_id, trace_id)
                    return
                
                elif status in ("FAILED", "CANCELED"):
                    await self._handle_merge_failure(merge_job_id, status, trace_id)
                    return
                
                # Still running - continue monitoring
            
            # Timeout reached
            self.logger.error(
                "Merge job monitoring timeout",
                merge_job_id=merge_job_id,
                flink_job_id=flink_job_id,
                elapsed_seconds=elapsed,
                trace_id=trace_id
            )
            
            await self.db.execute_raw_sql(f"""
                UPDATE s3_merge_jobs
                SET status = 'failed',
                    error_message = 'Monitoring timeout after {max_duration}s'
                WHERE merge_job_id = '{merge_job_id}'
            """)
        
        except Exception as e:
            self.logger.error(
                "Error during merge job monitoring",
                merge_job_id=merge_job_id,
                flink_job_id=flink_job_id,
                error=str(e),
                trace_id=trace_id
            )
    
    async def _get_flink_job_status(self, flink_job_id: str) -> str:
        """Query Flink job status via REST API"""
        
        url = f"{self.config.jobmanager_url}/jobs/{flink_job_id}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 404:
                        return "NOT_FOUND"
                    
                    if response.status != 200:
                        return "UNKNOWN"
                    
                    result = await response.json()
                    return result.get("state", "UNKNOWN")
        
        except Exception as e:
            self.logger.warning(
                "Failed to query Flink job status",
                flink_job_id=flink_job_id,
                error=str(e)
            )
            return "UNKNOWN"
    
    async def _handle_merge_success(self, merge_job_id: str, trace_id: str):
        """Handle successful merge completion"""
        
        # Update merge job record
        await self.db.execute_raw_sql(f"""
            UPDATE s3_merge_jobs
            SET status = 'completed',
                completed_at = SYSDATETIMEOFFSET()
            WHERE merge_job_id = '{merge_job_id}'
        """)
        
        self.logger.info(
            "Merge job completed successfully",
            merge_job_id=merge_job_id,
            trace_id=trace_id
        )
    
    async def _handle_merge_failure(
        self,
        merge_job_id: str,
        status: str,
        trace_id: str
    ):
        """Handle merge job failure"""
        
        # Update merge job record
        await self.db.execute_raw_sql(f"""
            UPDATE s3_merge_jobs
            SET status = 'failed',
                error_message = 'Flink job status: {status}',
                completed_at = SYSDATETIMEOFFSET()
            WHERE merge_job_id = '{merge_job_id}'
        """)
        
        self.logger.error(
            "Merge job failed",
            merge_job_id=merge_job_id,
            flink_status=status,
            trace_id=trace_id
        )
