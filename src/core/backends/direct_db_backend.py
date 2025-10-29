# src/core/backends/direct_db_backend.py
"""
Direct database backend for single-process deployment mode.
Worker and Orchestrator in same process, share DatabaseInterface instance.

CORRECTED: report_task_progress uses primitives and notes Kafka routing (Addendum 3.1.1, 3.1.2)
"""
import asyncio
from typing import Optional, Dict, Any
from core.interfaces.worker_backend_interface import IWorkerBackend
from core.models.models import WorkPacket
from core.config.config_models import DataSourceConfiguration
from core.db.database_interface import DatabaseInterface
from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler, ProcessingError, ConfigurationError, ErrorType

class DirectDBBackend(IWorkerBackend):
    """
    Implementation using direct DatabaseInterface calls.
    Used when deployment_model = "SINGLE_PROCESS"
    """
    
    def __init__(
        self,
        db_interface: DatabaseInterface,
        orchestrator_instance: Any,  # Reference to Orchestrator object
        logger: SystemLogger,
        error_handler: ErrorHandler
    ):
        self.db = db_interface
        self.orchestrator = orchestrator_instance
        self.logger = logger
        self.error_handler = error_handler
    
    async def get_next_task(
        self, 
        worker_id: str, 
        node_group: str
    ) -> Optional[WorkPacket]:
        """
        Direct call to orchestrator's task queue.
        
        Logic:
        1. Call orchestrator.get_task_for_worker(worker_id, node_group)
        2. Orchestrator checks in-memory task cache
        3. Returns WorkPacket or None
        
        Error Handling:
        - Any exception logged but returns None (worker will retry)
        """
        try:
            work_packet = await self.orchestrator.get_task_for_worker_async(
                worker_id, 
                node_group
            )
            return work_packet
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "direct_backend_get_next_task",
                worker_id=worker_id,
                node_group=node_group
            )
            return None
    
    async def report_task_completion(
        self,
        task_id: str,
        status: str,
        result_payload: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Direct call to orchestrator's result handler.
        
        Logic:
        1. Call orchestrator.report_task_result_async()
        2. Orchestrator updates task status in DB
        3. Updates in-memory job state
        4. Triggers JobCompletionMonitor check if job done
        
        Error Handling:
        - Retry 3 times with exponential backoff
        - If all retries fail, raise ProcessingError
        - Orchestrator MUST receive completion or task will be stuck
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self.orchestrator.report_task_result_async(
                    task_id=task_id,
                    status=status,
                    is_retryable=(status == "FAILED"),
                    result_payload=result_payload,
                    context=context
                )
                self.logger.info(
                    f"Task {task_id} completion reported successfully",
                    **context
                )
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    error = self.error_handler.handle_error(
                        e,
                        "direct_backend_report_completion",
                        task_id=task_id,
                        **context
                    )
                    raise ProcessingError(
                        f"Failed to report task completion after {max_retries} attempts",
                        ErrorType.PROCESSING_CRITICAL_FAILURE,
                        task_id=task_id
                    )
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    async def report_task_progress(
        self,
        job_id: int,
        task_id: str,
        output_type: str,
        output_payload: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        CORRECTED: Uses primitives instead of TaskOutputRecord model.
        
        NOTE: In the new architecture, this method is handled by Kafka integration.
        TaskOutputRecords are sent to Kafka topic 'prod_task_output_records',
        consumed by Flink, and written to the database.
        
        For single-process mode, we maintain backward compatibility by writing
        directly to the database. In production EKS mode, this will route through
        Kafka (handled by worker's Kafka producer).
        
        Logic:
        1. Convert task_id hex to bytes
        2. Call db.write_task_output_record()
        3. Record written to task_output_records table
        4. Pipeliner picks it up in next cycle
        
        Error Handling:
        - NO retries - if this fails, task must fail
        - Pipeliner depends on this for workflow continuation
        """
        try:
            task_id_bytes = bytes.fromhex(task_id)
            await self.db.write_task_output_record(
                job_id=job_id,
                task_id=task_id_bytes,
                output_type=output_type,
                payload=output_payload,
                context=context
            )
            self.logger.info(
                f"TaskOutputRecord of type '{output_type}' saved",
                **context
            )
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "direct_backend_report_progress",
                operation="write_task_output_record",
                **context
            )
            self.logger.critical(
                "FATAL: Failed to save TaskOutputRecord - workflow will break",
                error_id=error.error_id,
                **context
            )
            raise  # Re-raise to fail the task
    
    async def send_heartbeat(
        self,
        worker_id: str,
        task_id: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Direct call to orchestrator heartbeat handler.
        
        Logic:
        1. Call orchestrator.report_heartbeat_async()
        2. Orchestrator renews lease_expiry in tasks table
        
        Error Handling:
        - Log warning on failure but don't raise
        - Worst case: Task lease expires, reaper requeues (acceptable)
        """
        try:
            await self.orchestrator.report_heartbeat_async(
                task_id,
                {"worker_id": worker_id}
            )
            self.logger.debug(
                f"Heartbeat sent for task {task_id}",
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "direct_backend_heartbeat",
                **context
            )
            self.logger.warning(
                "Failed to send heartbeat - lease may expire",
                **context
            )
    
    async def get_datasource_configuration(
        self,
        datasource_id: str,
        context: Dict[str, Any]
    ) -> DataSourceConfiguration:
        """
        Fetches datasource config from database.
        
        Logic:
        1. Call db.get_datasource_configuration()
        2. Returns DataSourceConfiguration Pydantic model
        3. Model validated automatically (Pydantic)
        
        Error Handling:
        - If not found, raise ConfigurationError
        - If validation fails, raise ConfigurationError
        """
        try:
            config = await self.db.get_datasource_configuration(
                datasource_id,
                context=context
            )
            if not config:
                raise ConfigurationError(
                    f"Datasource '{datasource_id}' not found",
                    ErrorType.CONFIGURATION_MISSING,
                    datasource_id=datasource_id
                )
            return config
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "direct_backend_get_datasource_config",
                **context
            )
            raise
    
    async def get_connector_configuration(
        self,
        connector_type: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches connector config from database.
        
        Logic:
        1. Call db.get_connector_configuration(connector_type)
        2. Returns dict with query_sets, settings
        
        Error Handling:
        - If not found, raise ConfigurationError
        """
        try:
            config = await self.db.get_connector_configuration(
                connector_type,
                context=context
            )
            if not config:
                raise ConfigurationError(
                    f"Connector config for '{connector_type}' not found",
                    ErrorType.CONFIGURATION_MISSING,
                    connector_type=connector_type
                )
            return config
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "direct_backend_get_connector_config",
                **context
            )
            raise
    
    async def get_credential_reference(
        self,
        credential_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches credential reference from database.
        
        Logic:
        1. Call db.get_credential_by_id_async(credential_id)
        2. Returns metadata (NOT actual secret)
        3. Actual secret retrieved later via SecurityManager/CredentialManager
        
        Error Handling:
        - If not found, raise ConfigurationError
        - If worker not authorized, raise RightsError (future: RBAC check)
        """
        try:
            cred_ref = await self.db.get_credential_by_id_async(
                credential_id,
                context=context
            )
            if not cred_ref:
                raise ConfigurationError(
                    f"Credential '{credential_id}' not found",
                    ErrorType.CONFIGURATION_MISSING,
                    credential_id=credential_id
                )
            return cred_ref
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "direct_backend_get_credential",
                **context
            )
            raise
    
    async def get_classifier_template(
        self,
        template_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches full classifier template from database.
        
        Logic:
        1. Call db.get_classifier_template_full(template_id)
        2. Returns ORM object with all relationships loaded
        3. Convert to dict format expected by EngineInterface
        
        Error Handling:
        - If not found, raise ConfigurationError
        """
        try:
            template = await self.db.get_classifier_template_full(
                template_id,
                context=context
            )
            if not template:
                raise ConfigurationError(
                    f"Classifier template '{template_id}' not found",
                    ErrorType.CONFIGURATION_MISSING,
                    template_id=template_id
                )
            # Convert ORM to dict (EngineInterface expects dict)
            # This conversion already exists in _ConfigurationLoader
            return template
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "direct_backend_get_template",
                **context
            )
            raise
    
    async def register_worker(
        self,
        worker_id: str,
        node_group: str,
        capabilities: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Registers worker in component_registry table.
        
        Logic:
        1. UPSERT into component_registry
        2. Store worker_id, node_group, version, capabilities, last_heartbeat
        
        Error Handling:
        - Log error but don't fail worker startup
        - Worker can still function without registry
        """
        try:
            await self.db.upsert_component_registration(
                component_id=worker_id,
                component_type="worker",
                node_group=node_group,
                version=capabilities.get("version"),
                capabilities=capabilities,
                context=context
            )
            self.logger.info(
                f"Worker {worker_id} registered successfully",
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "direct_backend_register_worker",
                **context
            )
            self.logger.warning(
                "Worker registration failed but continuing startup",
                **context
            )
    
    async def deregister_worker(
        self,
        worker_id: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Removes worker from component_registry.
        
        Logic:
        1. DELETE from component_registry WHERE component_id = worker_id
        
        Error Handling:
        - Log error but don't prevent shutdown
        """
        try:
            await self.db.delete_component_registration(
                component_id=worker_id,
                context=context
            )
            self.logger.info(
                f"Worker {worker_id} deregistered successfully",
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "direct_backend_deregister_worker",
                **context
            )
