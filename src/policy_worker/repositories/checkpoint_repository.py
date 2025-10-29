"""
Repository for policy incremental checkpoint operations.
"""

from typing import Dict, Optional
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity

class CheckpointRepository:
    """
    Manages policy_incremental_checkpoints table for incremental policy execution.
    """
    
    def __init__(self, db_interface, logger: SystemLogger):
        self.db = db_interface
        self.logger = logger
    
    async def get_checkpoints(
        self,
        policy_template_id: int
    ) -> Dict[str, Dict]:
        """
        Get all checkpoints for a policy template.
        
        Returns:
            Dict mapping datasource_id to checkpoint data
        """
        
        query = f"""
            SELECT 
                datasource_id,
                last_metascan_checkpoint,
                last_classification_checkpoint,
                last_delta_classification_checkpoint,
                last_policy_execution_timestamp,
                last_processed_job_id
            FROM policy_incremental_checkpoints
            WHERE policy_template_id = {policy_template_id}
        """
        
        results = await self.db.execute_raw_sql(query)
        
        checkpoint_map = {}
        for row in results:
            checkpoint_map[row["datasource_id"]] = {
                "last_metascan_checkpoint": row.get("last_metascan_checkpoint"),
                "last_classification_checkpoint": row.get("last_classification_checkpoint"),
                "last_delta_classification_checkpoint": row.get("last_delta_classification_checkpoint"),
                "last_policy_execution_timestamp": row.get("last_policy_execution_timestamp"),
                "last_processed_job_id": row.get("last_processed_job_id")
            }
        
        return checkpoint_map
    
    async def upsert_checkpoint(
        self,
        policy_template_id: int,
        datasource_id: str,
        checkpoint_data: Dict
    ):
        """
        Insert or update checkpoint for policy + datasource.
        
        Args:
            policy_template_id: Policy template ID
            datasource_id: Datasource ID
            checkpoint_data: Dict with checkpoint fields to update
        """
        
        # Build SET clause dynamically
        set_clauses = []
        for key, value in checkpoint_data.items():
            if value is None:
                set_clauses.append(f"{key} = NULL")
            elif isinstance(value, datetime):
                set_clauses.append(f"{key} = '{value.isoformat()}'")
            elif isinstance(value, (int, float)):
                set_clauses.append(f"{key} = {value}")
            else:
                set_clauses.append(f"{key} = '{str(value).replace(chr(39), chr(39)*2)}'")
        
        set_clause = ", ".join(set_clauses)
        
        # MERGE statement for upsert
        query = f"""
            MERGE INTO policy_incremental_checkpoints AS target
            USING (
                SELECT {policy_template_id} AS policy_template_id,
                       '{datasource_id}' AS datasource_id
            ) AS source
            ON target.policy_template_id = source.policy_template_id
               AND target.datasource_id = source.datasource_id
            WHEN MATCHED THEN
                UPDATE SET 
                    {set_clause},
                    updated_at = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN
                INSERT (
                    policy_template_id,
                    datasource_id,
                    last_policy_execution_timestamp,
                    created_at,
                    updated_at
                )
                VALUES (
                    {policy_template_id},
                    '{datasource_id}',
                    SYSDATETIMEOFFSET(),
                    SYSDATETIMEOFFSET(),
                    SYSDATETIMEOFFSET()
                );
        """
        
        try:
            await self.db.execute_raw_sql(query)
            
            self.logger.debug(
                "Checkpoint upserted successfully",
                policy_template_id=policy_template_id,
                datasource_id=datasource_id
            )
        
        except Exception as e:
            raise ProcessingError(
                message=f"Failed to upsert checkpoint: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.MEDIUM,
                context={
                    "policy_template_id": policy_template_id,
                    "datasource_id": datasource_id
                },
                cause=e
            )
