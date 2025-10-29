"""
Handler for PREPARE_CLASSIFICATION_TASKS.
Implements delta logic and generates classification tasks with checkpointing.
"""

import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity
from core.models.models import WorkPacket, DiscoveredObject
from core.models.payloads import PrepareClassificationTasksPayload
from core.models.schema_models import SchemaFingerprint, compare_schemas

from ..utils.temp_table_manager import TempTableManager

class PrepareClassificationHandler:
    """
    Generates classification tasks with delta logic:
    - Structured: Compare schemas, identify new/changed tables
    - Unstructured: Compare last_modified vs last_classification_date
    
    Supports checkpointing for resumable execution on large datasets.
    """
    
    CHECKPOINT_INTERVAL = 100  # Save checkpoint every 100 items
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.db = policy_worker.db
        self.temp_table_mgr = TempTableManager(self.db, self.logger)
    
    async def process(self, work_packet: WorkPacket):
        """Generate classification tasks with delta logic and checkpointing"""
        
        payload = PrepareClassificationTasksPayload.model_validate(work_packet.payload)
        trace_id = work_packet.header.trace_id
        job_id = work_packet.header.job_id
        
        self.logger.info(
            "Processing PREPARE_CLASSIFICATION_TASKS",
            job_id=job_id,
            scan_type=payload.scan_type,
            scan_mode=payload.full_or_delta,
            datasource_count=len(payload.datasource_ids),
            trace_id=trace_id
        )
        
        # Check for existing checkpoint (recovery)
        checkpoint = await self._get_checkpoint(job_id)
        if checkpoint:
            self.logger.warning(
                "Resuming from checkpoint",
                job_id=job_id,
                checkpoint_boundary=checkpoint.get("boundary_id"),
                trace_id=trace_id
            )
        
        # Route based on scan type
        if payload.scan_type == "structured":
            await self._prepare_structured(job_id, payload, checkpoint, trace_id)
        else:
            await self._prepare_unstructured(job_id, payload, checkpoint, trace_id)
        
        # Clear checkpoint on success
        await self._clear_checkpoint(job_id)
        
        self.logger.info(
            "PREPARE_CLASSIFICATION_TASKS completed successfully",
            job_id=job_id,
            trace_id=trace_id
        )
    
    async def _prepare_structured(
        self,
        job_id: int,
        payload: PrepareClassificationTasksPayload,
        checkpoint: Optional[Dict],
        trace_id: str
    ):
        """Generate classification tasks for structured datasources (databases)"""
        
        self.logger.info(
            "Preparing classification tasks for structured datasources",
            job_id=job_id,
            scan_mode=payload.full_or_delta,
            trace_id=trace_id
        )
        
        # Get discovered tables from this job
        tables = await self.db.execute_raw_sql(f"""
            SELECT 
                do.object_key_hash,
                do.data_source_id as datasource_id,
                do.object_path,
                om.detailed_metadata
            FROM discovered_objects do
            INNER JOIN object_metadata om ON do.object_key_hash = om.object_key_hash
            WHERE do.job_id = {job_id}
              AND do.object_type = 'DATABASE_TABLE'
              AND do.data_source_id IN ({','.join([f"'{ds}'" for ds in payload.datasource_ids])})
            ORDER BY do.object_key_hash
        """)
        
        total_tables = len(tables)
        
        self.logger.info(
            "Tables retrieved for classification analysis",
            job_id=job_id,
            table_count=total_tables,
            trace_id=trace_id
        )
        
        if not tables:
            self.logger.warning(
                "No tables found for classification",
                job_id=job_id,
                trace_id=trace_id
            )
            return
        
        # Create temp table
        temp_table = f"temp_classification_tasks_{job_id}"
        await self.temp_table_mgr.create_temp_tasks_table(temp_table)
        
        self.logger.debug(
            "Temp table created for classification tasks",
            temp_table=temp_table,
            trace_id=trace_id
        )
        
        # Resume from checkpoint if exists
        start_idx = 0
        if checkpoint and checkpoint.get("boundary_type") == "page":
            page_num = int(checkpoint["boundary_id"].split("_")[1])
            start_idx = page_num * self.CHECKPOINT_INTERVAL
            self.logger.info(
                "Resuming from checkpoint",
                start_index=start_idx,
                trace_id=trace_id
            )
        
        # Process tables
        tasks_created = 0
        
        for idx, table_row in enumerate(tables[start_idx:], start=start_idx):
            # Determine if table needs classification
            needs_scan, columns_to_scan = await self._check_table_needs_scan(
                table_row=table_row,
                scan_mode=payload.full_or_delta,
                trace_id=trace_id
            )
            
            if needs_scan:
                # Create classification task
                task = self._create_classification_task(
                    job_id=job_id,
                    table_row=table_row,
                    classifier_template_id=payload.classifier_template_id,
                    columns_to_scan=columns_to_scan
                )
                
                await self.temp_table_mgr.bulk_insert_tasks(temp_table, [task])
                tasks_created += 1
            
            # Checkpoint every N tables
            if idx > 0 and idx % self.CHECKPOINT_INTERVAL == 0:
                await self._save_checkpoint(
                    job_id=job_id,
                    boundary_type="page",
                    boundary_id=f"page_{idx // self.CHECKPOINT_INTERVAL}",
                    checkpoint_data={"tables_processed": idx}
                )
                
                self.logger.log_progress_batch(
                    task_output_payload={
                        "batch_id": f"classification_prep_{idx}",
                        "count": self.CHECKPOINT_INTERVAL
                    },
                    task_id=job_id,
                    sampling_rate=0.1,
                    trace_id=trace_id
                )
        
        # Atomic commit
        await self.temp_table_mgr.commit_tasks_to_main(temp_table)
        
        self.logger.info(
            "Classification tasks generated for structured datasources",
            job_id=job_id,
            total_tables=total_tables,
            tasks_created=tasks_created,
            trace_id=trace_id
        )
    
    async def _prepare_unstructured(
        self,
        job_id: int,
        payload: PrepareClassificationTasksPayload,
        checkpoint: Optional[Dict],
        trace_id: str
    ):
        """Generate classification tasks for unstructured datasources (files)"""
        
        self.logger.info(
            "Preparing classification tasks for unstructured datasources",
            job_id=job_id,
            scan_mode=payload.full_or_delta,
            trace_id=trace_id
        )
        
        # Get discovered files from this job
        files = await self.db.execute_raw_sql(f"""
            SELECT 
                do.object_key_hash,
                do.data_source_id as datasource_id,
                do.object_path,
                do.last_modified,
                do.size_bytes
            FROM discovered_objects do
            WHERE do.job_id = {job_id}
              AND do.object_type = 'FILE'
              AND do.data_source_id IN ({','.join([f"'{ds}'" for ds in payload.datasource_ids])})
            ORDER BY do.object_key_hash
        """)
        
        total_files = len(files)
        
        self.logger.info(
            "Files retrieved for classification analysis",
            job_id=job_id,
            file_count=total_files,
            trace_id=trace_id
        )
        
        if not files:
            self.logger.warning(
                "No files found for classification",
                job_id=job_id,
                trace_id=trace_id
            )
            return
        
        # Create temp table
        temp_table = f"temp_classification_tasks_{job_id}"
        await self.temp_table_mgr.create_temp_tasks_table(temp_table)
        
        # Resume from checkpoint if exists
        start_idx = 0
        if checkpoint and checkpoint.get("boundary_type") == "page":
            page_num = int(checkpoint["boundary_id"].split("_")[1])
            start_idx = page_num * self.CHECKPOINT_INTERVAL
        
        # Process files
        tasks_created = 0
        
        for idx, file_row in enumerate(files[start_idx:], start=start_idx):
            # Check if file needs classification (delta logic)
            needs_scan = await self._check_file_needs_scan(
                file_row=file_row,
                scan_mode=payload.full_or_delta,
                trace_id=trace_id
            )
            
            if needs_scan:
                # Create classification task
                task = self._create_classification_task(
                    job_id=job_id,
                    file_row=file_row,
                    classifier_template_id=payload.classifier_template_id,
                    columns_to_scan=[]  # Not applicable for files
                )
                
                await self.temp_table_mgr.bulk_insert_tasks(temp_table, [task])
                tasks_created += 1
            
            # Checkpoint every N files
            if idx > 0 and idx % self.CHECKPOINT_INTERVAL == 0:
                await self._save_checkpoint(
                    job_id=job_id,
                    boundary_type="page",
                    boundary_id=f"page_{idx // self.CHECKPOINT_INTERVAL}",
                    checkpoint_data={"files_processed": idx}
                )
                
                self.logger.log_progress_batch(
                    task_output_payload={
                        "batch_id": f"classification_prep_{idx}",
                        "count": self.CHECKPOINT_INTERVAL
                    },
                    task_id=job_id,
                    sampling_rate=0.1,
                    trace_id=trace_id
                )
        
        # Atomic commit
        await self.temp_table_mgr.commit_tasks_to_main(temp_table)
        
        self.logger.info(
            "Classification tasks generated for unstructured datasources",
            job_id=job_id,
            total_files=total_files,
            tasks_created=tasks_created,
            trace_id=trace_id
        )
    
    async def _check_table_needs_scan(
        self,
        table_row: Dict,
        scan_mode: str,
        trace_id: str
    ) -> Tuple[bool, List[str]]:
        """
        Determine if table needs classification (delta logic).
        
        Returns:
            Tuple[bool, List[str]]: (needs_scan, columns_to_scan)
        """
        if scan_mode == "full":
            # Full scan: classify everything
            return True, []
        
        # Delta scan logic
        object_hash = table_row["object_key_hash"]
        
        # Get last classification date from LATEST/MERGED
        last_class_date = await self._get_last_classification_date(object_hash)
        
        if not last_class_date:
            # Never classified
            self.logger.debug(
                "Table never classified, needs scan",
                object_hash=object_hash.hex() if isinstance(object_hash, bytes) else object_hash,
                trace_id=trace_id
            )
            return True, []
        
        # Check schema changes (MVP: simple comparison)
        current_schema_json = table_row.get("detailed_metadata", {}).get("schema_info")
        if not current_schema_json:
            # No schema info - scan to be safe
            return True, []
        
        try:
            current_schema = SchemaFingerprint.model_validate(current_schema_json)
        except:
            self.logger.warning(
                "Failed to parse current schema, will classify",
                object_hash=object_hash.hex() if isinstance(object_hash, bytes) else object_hash,
                trace_id=trace_id
            )
            return True, []
        
        # Get last schema from LATEST/MERGED
        last_schema = await self._get_last_schema(object_hash)
        if not last_schema:
            # No previous schema - scan
            return True, []
        
        # Compare schemas
        comparison = compare_schemas(last_schema, current_schema)
        
        if comparison.has_changes:
            self.logger.debug(
                "Schema changes detected",
                object_hash=object_hash.hex() if isinstance(object_hash, bytes) else object_hash,
                added_columns=len(comparison.added_columns),
                modified_columns=len(comparison.modified_columns),
                trace_id=trace_id
            )
            
            # Return columns that need scanning
            columns_to_scan = comparison.added_columns + comparison.modified_columns
            return True, columns_to_scan
        
        # No changes
        return False, []
    
    async def _check_file_needs_scan(
        self,
        file_row: Dict,
        scan_mode: str,
        trace_id: str
    ) -> bool:
        """Determine if file needs classification (delta logic)"""
        
        if scan_mode == "full":
            return True
        
        # Delta scan: Compare last_modified vs last_classification_date
        object_hash = file_row["object_key_hash"]
        last_modified = file_row.get("last_modified")
        
        if not last_modified:
            # No modification date - scan to be safe
            return True
        
        # Get last classification date
        last_class_date = await self._get_last_classification_date(object_hash)
        
        if not last_class_date:
            # Never classified
            return True
        
        # Check if modified since last classification
        if last_modified > last_class_date:
            self.logger.debug(
                "File modified since last classification",
                object_hash=object_hash.hex() if isinstance(object_hash, bytes) else object_hash,
                last_modified=last_modified.isoformat(),
                last_classification=last_class_date.isoformat(),
                trace_id=trace_id
            )
            return True
        
        return False
    
    async def _get_last_classification_date(self, object_key_hash: bytes) -> Optional[datetime]:
        """Get last classification date from LATEST/MERGED data"""
        
        # Query scan_finding_summaries for this object
        result = await self.db.execute_raw_sql(f"""
            SELECT MAX(scan_timestamp) as last_classification
            FROM scan_finding_summaries
            WHERE object_key_hash = 0x{object_key_hash.hex()}
        """)
        
        if result and result[0]["last_classification"]:
            return result[0]["last_classification"]
        
        return None
    
    async def _get_last_schema(self, object_key_hash: bytes) -> Optional[SchemaFingerprint]:
        """Get last schema from LATEST/MERGED object_metadata"""
        
        result = await self.db.execute_raw_sql(f"""
            SELECT detailed_metadata
            FROM object_metadata
            WHERE object_key_hash = 0x{object_key_hash.hex()}
        """)
        
        if result and result[0].get("detailed_metadata"):
            schema_json = result[0]["detailed_metadata"].get("schema_info")
            if schema_json:
                try:
                    return SchemaFingerprint.model_validate(schema_json)
                except:
                    pass
        
        return None
    
    def _create_classification_task(
        self,
        job_id: int,
        table_row: Dict = None,
        file_row: Dict = None,
        classifier_template_id: str = None,
        columns_to_scan: List[str] = None
    ) -> Dict:
        """Create classification task dict"""
        
        import uuid
        
        row = table_row or file_row
        task_id = uuid.uuid4().bytes
        
        # Build discovered object
        discovered_object = {
            "object_key_hash": row["object_key_hash"],
            "object_path": row["object_path"],
            "datasource_id": row["datasource_id"],
            "object_type": "DATABASE_TABLE" if table_row else "FILE",
            "size_bytes": row.get("size_bytes"),
            "last_modified": row.get("last_modified").isoformat() if row.get("last_modified") else None
        }
        
        # Create work packet
        work_packet = {
            "header": {
                "task_id": task_id.hex(),
                "job_id": job_id,
                "trace_id": f"trace_{uuid.uuid4().hex}",
                "created_timestamp": datetime.now(timezone.utc).isoformat()
            },
            "config": {
                "batch_write_size": 1000,
                "max_content_size_mb": 100,
                "retry_count": 0
            },
            "payload": {
                "task_type": "CLASSIFICATION",
                "datasource_id": row["datasource_id"],
                "classifier_template_id": classifier_template_id,
                "discovered_objects": [discovered_object],
                "columns_to_scan": columns_to_scan or []  # Delta-specific
            }
        }
        
        return {
            "id": task_id,
            "job_id": job_id,
            "datasource_id": row["datasource_id"],
            "task_type": "CLASSIFICATION",
            "status": "PENDING",
            "eligible_worker_type": "DATACENTER",
            "node_group": "datacenter",
            "work_packet": work_packet,
            "created_at": datetime.now(timezone.utc)
        }
    
    async def _get_checkpoint(self, job_id: int) -> Optional[Dict]:
        """Get existing checkpoint for recovery"""
        
        result = await self.db.execute_raw_sql(f"""
            SELECT boundary_type, boundary_id, checkpoint_data
            FROM task_execution_checkpoints
            WHERE task_id = (
                SELECT id FROM tasks WHERE job_id = {job_id} 
                AND task_type = 'PREPARE_CLASSIFICATION_TASKS'
                LIMIT 1
            )
        """)
        
        if result:
            import json
            return {
                "boundary_type": result[0]["boundary_type"],
                "boundary_id": result[0]["boundary_id"],
                **json.loads(result[0]["checkpoint_data"] or "{}")
            }
        
        return None
    
    async def _save_checkpoint(
        self,
        job_id: int,
        boundary_type: str,
        boundary_id: str,
        checkpoint_data: Dict
    ):
        """Save checkpoint for resumable execution"""
        
        import json
        
        # Get task_id
        task_result = await self.db.execute_raw_sql(f"""
            SELECT id FROM tasks 
            WHERE job_id = {job_id} 
              AND task_type = 'PREPARE_CLASSIFICATION_TASKS'
            LIMIT 1
        """)
        
        if not task_result:
            return
        
        task_id = task_result[0]["id"]
        
        # Upsert checkpoint
        await self.db.execute_raw_sql(f"""
            MERGE INTO task_execution_checkpoints AS target
            USING (SELECT 0x{task_id.hex()} AS task_id) AS source
            ON target.task_id = source.task_id
            WHEN MATCHED THEN
                UPDATE SET 
                    boundary_type = '{boundary_type}',
                    boundary_id = '{boundary_id}',
                    checkpoint_data = '{json.dumps(checkpoint_data)}',
                    created_at = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN
                INSERT (task_id, boundary_type, boundary_id, checkpoint_data, created_at)
                VALUES (0x{task_id.hex()}, '{boundary_type}', '{boundary_id}', 
                        '{json.dumps(checkpoint_data)}', SYSDATETIMEOFFSET());
        """)
    
    async def _clear_checkpoint(self, job_id: int):
        """Clear checkpoint after successful completion"""
        
        await self.db.execute_raw_sql(f"""
            DELETE FROM task_execution_checkpoints
            WHERE task_id IN (
                SELECT id FROM tasks 
                WHERE job_id = {job_id} 
                  AND task_type = 'PREPARE_CLASSIFICATION_TASKS'
            )
        """)
