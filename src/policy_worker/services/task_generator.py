"""
Task generation service for policy actions and classification tasks.
Uses temp tables for atomic task creation at scale.
"""

import uuid
from typing import List, Dict, Any
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity
from core.models.models import ActionDefinition, ActionType
from core.models.payloads import PolicyQueryExecutePayload

from ..utils.temp_table_manager import TempTableManager

class TaskGenerator:
    """
    Generates policy action tasks atomically using temporary tables.
    Supports large-scale task generation (100K+ tasks) with proper batching.
    """
    
    BATCH_SIZE = 1000
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.db = policy_worker.db
        self.temp_table_mgr = TempTableManager(self.db, self.logger)
    
    async def generate_action_tasks(
        self,
        job_id: int,
        query_results: List[Dict[str, Any]],
        action_definition: ActionDefinition,
        payload: PolicyQueryExecutePayload,
        trace_id: str
    ) -> int:
        """
        Generate POLICY_ACTION_EXECUTE tasks atomically.
        
        Returns: Number of tasks created
        """
        if not query_results:
            self.logger.info(
                "No objects matched policy query, no action tasks to create",
                job_id=job_id,
                policy_id=payload.policy_template_id,
                trace_id=trace_id
            )
            return 0
        
        temp_table_name = f"temp_policy_actions_{job_id}"
        
        self.logger.info(
            "Starting action task generation",
            job_id=job_id,
            policy_id=payload.policy_template_id,
            object_count=len(query_results),
            action_type=action_definition.action_type,
            trace_id=trace_id
        )
        
        try:
            # Create temp table
            await self.temp_table_mgr.create_temp_tasks_table(temp_table_name)
            
            self.logger.debug(
                "Temp table created",
                temp_table=temp_table_name,
                trace_id=trace_id
            )
            
            # Generate tasks in batches
            tasks_generated = 0
            
            for i in range(0, len(query_results), self.BATCH_SIZE):
                batch = query_results[i:i + self.BATCH_SIZE]
                
                tasks = []
                for result in batch:
                    task = self._create_action_task(
                        job_id=job_id,
                        result=result,
                        action_definition=action_definition,
                        payload=payload
                    )
                    tasks.append(task)
                
                # Bulk insert to temp table
                await self.temp_table_mgr.bulk_insert_tasks(temp_table_name, tasks)
                
                tasks_generated += len(tasks)
                
                self.logger.log_progress_batch(
                    task_output_payload={"batch_id": f"action_tasks_{i}", "count": len(tasks)},
                    task_id=job_id,
                    sampling_rate=0.1,
                    force_log=(i == 0),  # Always log first batch
                    trace_id=trace_id
                )
            
            # Atomic commit to main table
            await self.temp_table_mgr.commit_tasks_to_main(temp_table_name)
            
            self.logger.info(
                "Action tasks generated successfully",
                job_id=job_id,
                policy_id=payload.policy_template_id,
                tasks_created=tasks_generated,
                trace_id=trace_id
            )
            
            return tasks_generated
        
        except Exception as e:
            self.logger.error(
                "Failed to generate action tasks",
                job_id=job_id,
                policy_id=payload.policy_template_id,
                error=str(e),
                trace_id=trace_id
            )
            
            # Best-effort cleanup
            try:
                await self.temp_table_mgr.drop_temp_table(temp_table_name)
            except:
                pass
            
            raise ProcessingError(
                message=f"Action task generation failed: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.HIGH,
                context={
                    "job_id": job_id,
                    "policy_id": payload.policy_template_id,
                    "trace_id": trace_id
                },
                cause=e
            )
    
    def _create_action_task(
        self,
        job_id: int,
        result: Dict[str, Any],
        action_definition: ActionDefinition,
        payload: PolicyQueryExecutePayload
    ) -> Dict[str, Any]:
        """Create single action task dict"""
        
        # Generate task ID
        task_id = uuid.uuid4().bytes
        
        # Determine eligible worker type based on action
        eligible_worker_type = self._get_worker_type_for_action(action_definition.action_type)
        
        # Create work packet
        work_packet = {
            "header": {
                "task_id": task_id.hex(),
                "job_id": job_id,
                "parent_task_id": None,
                "trace_id": f"trace_{uuid.uuid4().hex}",
                "created_timestamp": datetime.now(timezone.utc).isoformat()
            },
            "config": {
                "batch_write_size": 1000,
                "max_content_size_mb": 100,
                "fetch_permissions": False,
                "retry_count": 0
            },
            "payload": {
                "task_type": "POLICY_ACTION_EXECUTE",
                "object_key_hash": result["object_key_hash"],
                "object_path": result["object_path"],
                "datasource_id": result.get("datasource_id"),
                "policy_id": str(payload.policy_template_id),
                "policy_name": payload.policy_name,
                "action": action_definition.model_dump()
            }
        }
        
        return {
            "id": task_id,
            "job_id": job_id,
            "datasource_id": result.get("datasource_id"),
            "task_type": "POLICY_ACTION_EXECUTE",
            "status": "PENDING",
            "eligible_worker_type": eligible_worker_type,
            "node_group": "datacenter",  # Actions execute on datacenter workers
            "work_packet": work_packet,
            "created_at": datetime.now(timezone.utc)
        }
    
    def _get_worker_type_for_action(self, action_type: ActionType) -> str:
        """Determine which worker type can execute this action"""
        # Connector-based actions: Datacenter workers
        if action_type in (ActionType.MOVE, ActionType.DELETE, ActionType.ENCRYPT, ActionType.MIP):
            return "DATACENTER"
        
        # Tagging: Central PolicyWorker (DB update only)
        elif action_type == ActionType.TAG:
            return "POLICY"
        
        else:
            return "DATACENTER"  # Default
