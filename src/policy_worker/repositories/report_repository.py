"""
Repository for report_data table operations.
"""

from datetime import datetime, timezone
from core.logging.system_logger import SystemLogger

class ReportRepository:
    """
    Manages report_data table for tracking generated reports.
    """
    
    def __init__(self, db_interface, logger: SystemLogger):
        self.db = db_interface
        self.logger = logger
    
    async def insert_report_data(
        self,
        job_id: int,
        policy_template_id: int,
        report_type: str,
        report_scope: str,
        s3_path: str,
        datasource_id: str = None,
        output_format: str = "parquet",
        row_count: int = None,
        file_size_bytes: int = None
    ) -> int:
        """
        Insert report metadata record.
        
        Returns: report_id
        """
        
        datasource_clause = f"'{datasource_id}'" if datasource_id else "NULL"
        row_count_clause = str(row_count) if row_count else "NULL"
        file_size_clause = str(file_size_bytes) if file_size_bytes else "NULL"
        
        query = f"""
            INSERT INTO report_data (
                job_id,
                policy_template_id,
                report_type,
                report_scope,
                s3_path,
                datasource_id,
                output_format,
                row_count,
                file_size_bytes,
                created_at
            )
            OUTPUT INSERTED.report_id
            VALUES (
                {job_id},
                {policy_template_id},
                '{report_type}',
                '{report_scope}',
                '{s3_path}',
                {datasource_clause},
                '{output_format}',
                {row_count_clause},
                {file_size_clause},
                SYSDATETIMEOFFSET()
            )
        """
        
        result = await self.db.execute_raw_sql(query)
        
        if result:
            report_id = result[0]["report_id"]
            
            self.logger.info(
                "Report metadata inserted",
                report_id=report_id,
                job_id=job_id,
                policy_template_id=policy_template_id,
                s3_path=s3_path
            )
            
            return report_id
        
        return None
