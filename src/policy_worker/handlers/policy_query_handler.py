"""
Handler for POLICY_QUERY_EXECUTE tasks.
Executes policy queries and generates action tasks.
"""

from datetime import datetime, timezone
from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity
from core.models.models import WorkPacket
from core.models.payloads import PolicyQueryExecutePayload

class PolicyQueryHandler:
    """
    Processes POLICY_QUERY_EXECUTE tasks:
    1. Execute policy query (via QueryExecutor)
    2. Generate action tasks (via TaskGenerator)
    3. Update incremental checkpoint if applicable
    """
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.query_executor = policy_worker.query_executor
        self.task_generator = policy_worker.task_generator
        self.checkpoint_repo = None  # Lazy init
    
    async def process(self, work_packet: WorkPacket):
        """Execute policy query and generate action tasks"""
        
        payload = PolicyQueryExecutePayload.model_validate(work_packet.payload)
        trace_id = work_packet.header.trace_id
        job_id = work_packet.header.job_id
        
        self.logger.info(
            "Processing POLICY_QUERY_EXECUTE task",
            job_id=job_id,
            policy_id=payload.policy_template_id,
            policy_name=payload.policy_name,
            query_source=payload.query_source,
            incremental=payload.incremental_mode,
            trace_id=trace_id
        )
        
        start_time = datetime.now()
        
        try:
            # Execute policy query
            query_results = await self.query_executor.execute_policy_query(
                payload=payload,
                trace_id=trace_id
            )
            
            self.logger.info(
                "Policy query returned results",
                job_id=job_id,
                policy_id=payload.policy_template_id,
                result_count=len(query_results),
                trace_id=trace_id
            )
            
            # Generate action tasks if results exist
            tasks_created = 0
            if query_results:
                tasks_created = await self.task_generator.generate_action_tasks(
                    job_id=job_id,
                    query_results=query_results,
                    action_definition=payload.action_definition,
                    payload=payload,
                    trace_id=trace_id
                )
            
            # Update incremental checkpoint if applicable
            if payload.incremental_mode:
                await self._update_incremental_checkpoints(payload, trace_id)
            
            elapsed_seconds = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                "POLICY_QUERY_EXECUTE completed successfully",
                job_id=job_id,
                policy_id=payload.policy_template_id,
                objects_matched=len(query_results),
                tasks_created=tasks_created,
                execution_time_seconds=round(elapsed_seconds, 2),
                trace_id=trace_id
            )
        
        except Exception as e:
            elapsed_seconds = (datetime.now() - start_time).total_seconds()
            
            self.logger.error(
                "POLICY_QUERY_EXECUTE failed",
                job_id=job_id,
                policy_id=payload.policy_template_id,
                execution_time_seconds=round(elapsed_seconds, 2),
                error=str(e),
                trace_id=trace_id
            )
            raise
    
    async def _update_incremental_checkpoints(
        self,
        payload: PolicyQueryExecutePayload,
        trace_id: str
    ):
        """Update checkpoint timestamps after successful execution"""
        
        if not self.checkpoint_repo:
            from ..repositories.checkpoint_repository import CheckpointRepository
            self.checkpoint_repo = CheckpointRepository(
                self.policy_worker.db,
                self.logger
            )
        
        # Get datasources that were processed
        datasource_ids = await self.policy_worker.db.get_datasources_by_storage_scope(
            payload.policy_storage_scope
        )
        
        # Get current timestamps
        ds_timestamps = await self.policy_worker.db.get_datasource_timestamps()
        
        # Update checkpoints
        for ds_id in datasource_ids:
            ds_ts = ds_timestamps.get(ds_id)
            if not ds_ts:
                continue
            
            checkpoint_data = {
                "last_policy_execution_timestamp": datetime.now(timezone.utc)
            }
            
            # Set appropriate checkpoint fields based on data source
            if payload.data_source in ("DISCOVEREDOBJECT", "OBJECTMETADATA"):
                if ds_ts.last_metascantimestamp:
                    checkpoint_data["last_metascan_checkpoint"] = ds_ts.last_metascantimestamp
            
            elif payload.data_source == "SCANFINDINGS":
                if ds_ts.last_classificationscantimestamp:
                    checkpoint_data["last_classification_checkpoint"] = ds_ts.last_classificationscantimestamp
                if ds_ts.last_deltaclassificationscantimestamp:
                    checkpoint_data["last_delta_classification_checkpoint"] = ds_ts.last_deltaclassificationscantimestamp
            
            await self.checkpoint_repo.upsert_checkpoint(
                policy_template_id=payload.policy_template_id,
                datasource_id=ds_id,
                checkpoint_data=checkpoint_data
            )
        
        self.logger.debug(
            "Incremental checkpoints updated",
            policy_id=payload.policy_template_id,
            datasource_count=len(datasource_ids),
            trace_id=trace_id
        )
