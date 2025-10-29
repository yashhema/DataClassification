"""
Utility for managing temporary tables for atomic task creation.
"""

from typing import List, Dict, Any
from core.logging.system_logger import SystemLogger
from core.errors import ProcessingError, ErrorType, ErrorSeverity

class TempTableManager:
    """
    Manages temporary tables for atomic bulk operations.
    Uses SQL Server temp table syntax (##global_temp or #local_temp).
    """
    
    def __init__(self, db_interface, logger: SystemLogger):
        self.db = db_interface
        self.logger = logger
    
    async def create_temp_tasks_table(self, temp_table_name: str):
        """
        Create temporary tasks table matching main tasks schema.
        Uses global temp table (##) for visibility across connections.
        """
        
        # Use ## prefix for global temp table
        table_name = f"##{temp_table_name}" if not temp_table_name.startswith("##") else temp_table_name
        
        create_sql = f"""
            CREATE TABLE {table_name} (
                id BINARY(16) PRIMARY KEY,
                job_id INT NOT NULL,
                datasource_id VARCHAR(255),
                task_type VARCHAR(100) NOT NULL,
                status VARCHAR(50) NOT NULL,
                eligible_worker_type VARCHAR(50),
                node_group VARCHAR(100),
                work_packet NVARCHAR(MAX) NOT NULL,
                created_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
                boundary_id VARCHAR(255)
            )
        """
        
        try:
            await self.db.execute_raw_sql(create_sql)
            
            self.logger.debug(
                "Temp tasks table created",
                table_name=table_name
            )
        
        except Exception as e:
            raise ProcessingError(
                message=f"Failed to create temp table: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.HIGH,
                context={"table_name": table_name},
                cause=e
            )
    
    async def bulk_insert_tasks(
        self,
        temp_table_name: str,
        tasks: List[Dict[str, Any]]
    ):
        """
        Bulk insert tasks to temp table.
        Uses batch INSERT statements for SQL Server.
        """
        
        if not tasks:
            return
        
        table_name = f"##{temp_table_name}" if not temp_table_name.startswith("##") else temp_table_name
        
        # Build batch INSERT
        # SQL Server supports multi-row INSERT: INSERT INTO ... VALUES (...), (...), ...
        
        import json
        
        values_parts = []
        for task in tasks:
            task_id_hex = task["id"].hex() if isinstance(task["id"], bytes) else task["id"]
            work_packet_json = json.dumps(task["work_packet"]).replace("'", "''")
            datasource_clause = f"'{task['datasource_id']}'" if task.get('datasource_id') else "NULL"
            eligible_worker_clause = f"'{task['eligible_worker_type']}'" if task.get('eligible_worker_type') else "NULL"
            boundary_clause = f"'{task['boundary_id']}'" if task.get('boundary_id') else "NULL"
            
            values_parts.append(f"""(
                0x{task_id_hex},
                {task['job_id']},
                {datasource_clause},
                '{task['task_type']}',
                '{task['status']}',
                {eligible_worker_clause},
                '{task.get('node_group', 'datacenter')}',
                N'{work_packet_json}',
                SYSDATETIMEOFFSET(),
                {boundary_clause}
            )""")
        
        # Batch in chunks of 1000 for SQL Server
        batch_size = 1000
        for i in range(0, len(values_parts), batch_size):
            batch_values = values_parts[i:i + batch_size]
            
            insert_sql = f"""
                INSERT INTO {table_name} (
                    id, job_id, datasource_id, task_type, status,
                    eligible_worker_type, node_group, work_packet,
                    created_at, boundary_id
                )
                VALUES {','.join(batch_values)}
            """
            
            try:
                await self.db.execute_raw_sql(insert_sql)
            except Exception as e:
                raise ProcessingError(
                    message=f"Failed to bulk insert tasks: {str(e)}",
                    error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                    severity=ErrorSeverity.HIGH,
                    context={
                        "table_name": table_name,
                        "batch_start": i,
                        "batch_size": len(batch_values)
                    },
                    cause=e
                )
        
        self.logger.debug(
            "Bulk insert completed",
            table_name=table_name,
            task_count=len(tasks)
        )
    
    async def commit_tasks_to_main(self, temp_table_name: str):
        """
        Atomically move tasks from temp table to main tasks table.
        Uses INSERT SELECT for efficient transfer.
        """
        
        table_name = f"##{temp_table_name}" if not temp_table_name.startswith("##") else temp_table_name
        
        commit_sql = f"""
            INSERT INTO tasks (
                id, job_id, datasource_id, task_type, status,
                eligible_worker_type, node_group, work_packet,
                created_at, boundary_id
            )
            SELECT 
                id, job_id, datasource_id, task_type, status,
                eligible_worker_type, node_group, work_packet,
                created_at, boundary_id
            FROM {table_name}
        """
        
        try:
            await self.db.execute_raw_sql(commit_sql)
            
            self.logger.info(
                "Tasks committed to main table",
                temp_table=table_name
            )
            
            # Drop temp table
            await self.drop_temp_table(temp_table_name)
        
        except Exception as e:
            raise ProcessingError(
                message=f"Failed to commit tasks: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.HIGH,
                context={"table_name": table_name},
                cause=e
            )
    
    async def drop_temp_table(self, temp_table_name: str):
        """Drop temporary table (cleanup)"""
        
        table_name = f"##{temp_table_name}" if not temp_table_name.startswith("##") else temp_table_name
        
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"
        
        try:
            await self.db.execute_raw_sql(drop_sql)
            
            self.logger.debug(
                "Temp table dropped",
                table_name=table_name
            )
        except Exception as e:
            self.logger.warning(
                "Failed to drop temp table (non-critical)",
                table_name=table_name,
                error=str(e)
            )
