"""
Query execution service for Athena and Yugabyte.
Handles query execution with retry logic, streaming results, and error handling.
"""

import asyncio
from typing import List, Dict, Any, Literal
from datetime import datetime

from core.logging.system_logger import SystemLogger
from core.errors import (
    NetworkError, ProcessingError, ErrorType, ErrorSeverity, ErrorHandler
)
from core.models.payloads import PolicyQueryExecutePayload
from core.models.field_mappings import DataSourceType

from .query_builder import QueryBuilder
from ..repositories.checkpoint_repository import CheckpointRepository

class QueryExecutor:
    """
    Executes policy queries against Athena or Yugabyte with proper error handling,
    retry logic, and incremental filtering.
    """
    
    def __init__(self, policy_worker):
        self.policy_worker = policy_worker
        self.logger: SystemLogger = policy_worker.logger
        self.error_handler: ErrorHandler = policy_worker.error_handler
        self.db = policy_worker.db
        self.checkpoint_repo = CheckpointRepository(policy_worker.db, policy_worker.logger)
    
    async def execute_policy_query(
        self,
        payload: PolicyQueryExecutePayload,
        trace_id: str
    ) -> List[Dict[str, Any]]:
        """
        Execute policy query with incremental filtering and proper error handling.
        
        Returns list of matching objects: [{"object_key_hash": bytes, "object_path": str, ...}]
        """
        start_time = datetime.now()
        
        self.logger.info(
            "Starting policy query execution",
            policy_id=payload.policy_template_id,
            policy_name=payload.policy_name,
            query_source=payload.query_source,
            data_source=payload.data_source,
            incremental=payload.incremental_mode,
            trace_id=trace_id
        )
        
        try:
            # Get target datasources (with incremental filtering)
            datasources = await self._get_target_datasources(payload)
            
            if not datasources:
                self.logger.info(
                    "No datasources with new data for incremental policy",
                    policy_id=payload.policy_template_id,
                    trace_id=trace_id
                )
                return []
            
            self.logger.info(
                "Target datasources identified",
                policy_id=payload.policy_template_id,
                datasource_count=len(datasources),
                datasources=datasources[:10],  # Log first 10
                trace_id=trace_id
            )
            
            # Build query
            query_builder = QueryBuilder(
                data_source=payload.data_source,
                backend=payload.query_source
            )
            
            sql_query, needs_policy_join = query_builder.build_query(
                selection_criteria=payload.selection_criteria,
                datasource_ids=datasources
            )
            
            self.logger.debug(
                "Query built successfully",
                policy_id=payload.policy_template_id,
                needs_policy_join=needs_policy_join,
                query_preview=sql_query[:500],  # First 500 chars
                trace_id=trace_id
            )
            
            # Execute query based on source
            if payload.query_source == "athena":
                results = await self._execute_athena_query(sql_query, trace_id)
            else:
                results = await self._execute_yugabyte_query(sql_query, trace_id)
            
            elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            self.logger.info(
                "Policy query completed successfully",
                policy_id=payload.policy_template_id,
                result_count=len(results),
                execution_time_ms=elapsed_ms,
                trace_id=trace_id
            )
            
            return results
        
        except Exception as e:
            elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            if isinstance(e, NetworkError):
                raise
            
            error = ProcessingError(
                message=f"Policy query execution failed: {str(e)}",
                error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                severity=ErrorSeverity.HIGH,
                context={
                    "policy_id": payload.policy_template_id,
                    "query_source": payload.query_source,
                    "execution_time_ms": elapsed_ms,
                    "trace_id": trace_id
                },
                cause=e
            )
            self.logger.log_classification_error(error, trace_id)
            raise error
    
    async def _get_target_datasources(
        self,
        payload: PolicyQueryExecutePayload
    ) -> List[str]:
        """
        Get datasources to query with incremental filtering.
        
        For incremental policies: Only returns datasources with new data since last execution.
        For one-shot policies: Returns all datasources matching storage scope.
        """
        # Get all datasources matching storage scope
        all_datasources = await self.db.get_datasources_by_storage_scope(
            payload.policy_storage_scope
        )
        
        if not payload.incremental_mode:
            return all_datasources
        
        # Incremental mode: Filter by checkpoint timestamps
        checkpoints = await self.checkpoint_repo.get_checkpoints(
            payload.policy_template_id
        )
        
        # Get datasource timestamps
        ds_timestamps = await self.db.get_datasource_timestamps()
        
        datasources_with_updates = []
        
        for ds_id in all_datasources:
            ds_ts = ds_timestamps.get(ds_id)
            if not ds_ts:
                # No timestamp info - include it
                datasources_with_updates.append(ds_id)
                continue
            
            checkpoint = checkpoints.get(ds_id)
            if not checkpoint:
                # Never processed - include it
                datasources_with_updates.append(ds_id)
                continue
            
            # Check if new data exists based on data source type
            has_new_data = False
            
            if payload.data_source in ("DISCOVEREDOBJECT", "OBJECTMETADATA"):
                if ds_ts.last_metascantimestamp:
                    if not checkpoint.last_metascan_checkpoint:
                        has_new_data = True
                    elif ds_ts.last_metascantimestamp > checkpoint.last_metascan_checkpoint:
                        has_new_data = True
            
            elif payload.data_source == "SCANFINDINGS":
                if ds_ts.last_classificationscantimestamp:
                    if not checkpoint.last_classification_checkpoint:
                        has_new_data = True
                    elif ds_ts.last_classificationscantimestamp > checkpoint.last_classification_checkpoint:
                        has_new_data = True
                
                # Also check delta classification
                if ds_ts.last_deltaclassificationscantimestamp:
                    if not checkpoint.last_delta_classification_checkpoint:
                        has_new_data = True
                    elif ds_ts.last_deltaclassificationscantimestamp > checkpoint.last_delta_classification_checkpoint:
                        has_new_data = True
            
            if has_new_data:
                datasources_with_updates.append(ds_id)
        
        self.logger.debug(
            "Incremental filtering complete",
            policy_id=payload.policy_template_id,
            total_datasources=len(all_datasources),
            datasources_with_updates=len(datasources_with_updates)
        )
        
        return datasources_with_updates
    
    async def _execute_athena_query(
        self,
        query: str,
        trace_id: str,
        max_attempts: int = 3
    ) -> List[Dict[str, Any]]:
        """Execute query via Athena with retry logic"""
        
        for attempt in range(max_attempts):
            try:
                self.logger.debug(
                    "Executing Athena query",
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    trace_id=trace_id
                )
                
                # Submit query via Athena client
                # (Assume athena_client is initialized in PolicyWorker)
                results = await self.policy_worker.athena_client.execute_query(query)
                
                return results
            
            except Exception as e:
                is_last_attempt = (attempt == max_attempts - 1)
                
                # Determine if retryable
                retryable = self._is_athena_error_retryable(e)
                
                if retryable and not is_last_attempt:
                    delay = 2 ** attempt + (asyncio.get_event_loop().time() % 1)
                    
                    self.logger.warning(
                        "Athena query failed, retrying",
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        retry_delay_seconds=round(delay, 2),
                        error=str(e),
                        trace_id=trace_id
                    )
                    
                    await asyncio.sleep(delay)
                    continue
                
                # Not retryable or last attempt - raise error
                error = NetworkError(
                    message=f"Athena query failed: {str(e)}",
                    error_type=ErrorType.NETWORK_TIMEOUT if "timeout" in str(e).lower() 
                               else ErrorType.NETWORK_CONNECTION_FAILED,
                    severity=ErrorSeverity.HIGH,
                    retryable=retryable and not is_last_attempt,
                    context={"trace_id": trace_id, "attempt": attempt + 1},
                    cause=e
                )
                raise error
    
    async def _execute_yugabyte_query(
        self,
        query: str,
        trace_id: str,
        max_attempts: int = 3
    ) -> List[Dict[str, Any]]:
        """Execute query via Yugabyte with retry logic"""
        
        for attempt in range(max_attempts):
            try:
                self.logger.debug(
                    "Executing Yugabyte query",
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    trace_id=trace_id
                )
                
                # Execute via DatabaseInterface
                results = await self.db.execute_raw_sql(query)
                
                # Convert to list of dicts
                return [dict(row) for row in results]
            
            except Exception as e:
                is_last_attempt = (attempt == max_attempts - 1)
                
                retryable = self._is_db_error_retryable(e)
                
                if retryable and not is_last_attempt:
                    delay = 2 ** attempt + (asyncio.get_event_loop().time() % 1)
                    
                    self.logger.warning(
                        "Yugabyte query failed, retrying",
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        retry_delay_seconds=round(delay, 2),
                        error=str(e),
                        trace_id=trace_id
                    )
                    
                    await asyncio.sleep(delay)
                    continue
                
                error = NetworkError(
                    message=f"Yugabyte query failed: {str(e)}",
                    error_type=ErrorType.NETWORK_CONNECTION_FAILED,
                    severity=ErrorSeverity.HIGH,
                    retryable=retryable and not is_last_attempt,
                    context={"trace_id": trace_id, "attempt": attempt + 1},
                    cause=e
                )
                raise error
    
    def _is_athena_error_retryable(self, error: Exception) -> bool:
        """Determine if Athena error is transient"""
        error_str = str(error).lower()
        retryable_patterns = [
            "timeout",
            "throttling",
            "too many requests",
            "service unavailable",
            "internal error"
        ]
        return any(pattern in error_str for pattern in retryable_patterns)
    
    def _is_db_error_retryable(self, error: Exception) -> bool:
        """Determine if database error is transient"""
        error_str = str(error).lower()
        retryable_patterns = [
            "connection",
            "timeout",
            "deadlock",
            "lock wait",
            "temporary"
        ]
        return any(pattern in error_str for pattern in retryable_patterns)
