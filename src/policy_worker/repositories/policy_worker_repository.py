"""
PolicyWorker-specific database operations.
Extends DatabaseInterface patterns for policy execution needs.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from core.logging.system_logger import SystemLogger

class PolicyWorkerRepository:
    """
    Repository for PolicyWorker database operations.
    Uses DatabaseInterface for actual execution (dependency injection).
    """
    
    def __init__(self, db_interface, logger: SystemLogger):
        self.db = db_interface
        self.logger = logger
    
    async def get_datasources_by_storage_scope(
        self,
        storage_scope: str
    ) -> List[str]:
        """
        Get datasource IDs matching policy storage scope.
        
        Args:
            storage_scope: 'S3', 'DB', or 'BOTH'
        
        Returns:
            List of datasource_id strings
        """
        
        # Map storage scope to storage preference values
        if storage_scope == "S3":
            filter_clause = "storage_preference = 'S3'"
        elif storage_scope == "DB":
            filter_clause = "storage_preference = 'DB'"
        elif storage_scope == "BOTH":
            filter_clause = "storage_preference IN ('S3', 'DB', 'BOTH')"
        else:
            filter_clause = "1=1"  # All datasources
        
        query = f"""
            SELECT datasource_id
            FROM datasources
            WHERE {filter_clause}
              AND status = 'active'
            ORDER BY datasource_id
        """
        
        results = await self.db.execute_raw_sql(query)
        
        return [row["datasource_id"] for row in results]
    
    async def get_datasource_timestamps(self) -> Dict[str, Any]:
        """
        Get scan timestamps for all datasources.
        
        Returns:
            Dict mapping datasource_id to timestamp object with:
                - last_metascantimestamp
                - last_classificationscantimestamp
                - last_deltaclassificationscantimestamp
        """
        
        query = """
            SELECT 
                datasource_id,
                last_metascantimestamp,
                last_classificationscantimestamp,
                last_deltaclassificationscantimestamp
            FROM datasources
            WHERE status = 'active'
        """
        
        results = await self.db.execute_raw_sql(query)
        
        # Convert to dict
        timestamp_map = {}
        for row in results:
            timestamp_map[row["datasource_id"]] = {
                "last_metascantimestamp": row.get("last_metascantimestamp"),
                "last_classificationscantimestamp": row.get("last_classificationscantimestamp"),
                "last_deltaclassificationscantimestamp": row.get("last_deltaclassificationscantimestamp")
            }
        
        return timestamp_map
    
    async def get_policy_template(self, policy_template_id: int) -> Optional[Dict]:
        """Get policy template configuration"""
        
        query = f"""
            SELECT 
                id,
                name,
                policy_type,
                configuration
            FROM policy_templates
            WHERE id = {policy_template_id}
        """
        
        results = await self.db.execute_raw_sql(query)
        
        if results:
            return dict(results[0])
        
        return None
    
    async def execute_raw_sql(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Execute raw SQL query (wrapper for DatabaseInterface).
        
        Args:
            query: SQL query string
            params: Optional parameters dict
        
        Returns:
            List of result rows as dicts
        """
        return await self.db.execute_raw_sql(query, params)
