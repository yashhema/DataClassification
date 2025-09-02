# src/worker/worker.py
"""
The Worker implementation that supports the defined interfaces for
orchestrator communication, connector usage, and classification engine integration.

This worker implements the contracts defined in:
- WorkerOrchestratorInterface.txt  
- worker_interfaces.py (IDataSourceConnector, IClassificationEngine)
"""

import time
import threading
import requests
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import traceback
from enum import Enum

from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.errors import ErrorHandler, ClassificationError, NetworkError, ProcessingError
from core.models.models import WorkPacket, TaskType, TaskOutputRecord, ProgressUpdate

# Add these imports at the top:
from core.interfaces.worker_interfaces import (
    IDatabaseDataSourceConnector, 
    IFileDataSourceConnector, 
    IClassificationEngine
)
from core.models.models import ContentComponent
class WorkerStatus(str, Enum):
    """Worker operational states."""
    STARTING = "STARTING"
    IDLE = "IDLE"
    PROCESSING_TASK = "PROCESSING_TASK"
    REPORTING_PROGRESS = "REPORTING_PROGRESS"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    STOPPED = "STOPPED"
    ERROR = "ERROR"

class Worker:
    """
    Main worker implementation that processes tasks from the orchestrator.
    Supports both single-process (in-memory) and distributed (API) modes.
    """

    def __init__(self, 
                 settings: SystemConfig, 
                 logger: SystemLogger, 
                 error_handler: ErrorHandler,
                 worker_id: Optional[str] = None,
                 orchestrator_url: Optional[str] = None,
                 orchestrator_instance: Optional[Any] = None):
        """
        Initialize worker with configuration and dependencies.
        
        Args:
            settings: System configuration
            logger: System logger instance
            error_handler: Error handler instance
            worker_id: Unique worker identifier
            orchestrator_url: URL for orchestrator API (EKS mode)
            orchestrator_instance: Direct orchestrator reference (single-process mode)
        """
        self.settings = settings
        self.logger = logger
        self.error_handler = error_handler
        self.worker_id = worker_id or f"worker-{int(time.time())}"
        
        # Deployment mode detection
        self.is_single_process_mode = self.settings.orchestrator.deployment_model.upper() == "SINGLE_PROCESS"
        
        # Orchestrator communication setup
        if self.is_single_process_mode:
            if not orchestrator_instance:
                raise ValueError("orchestrator_instance required for single-process mode")
            self.orchestrator = orchestrator_instance
            self.orchestrator_url = None
        else:
            if not orchestrator_url:
                raise ValueError("orchestrator_url required for distributed mode")
            self.orchestrator_url = orchestrator_url
            self.orchestrator = None

        # Worker state
        self.status = WorkerStatus.STARTING
        self.current_task: Optional[WorkPacket] = None
        self.shutdown_event = threading.Event()
        self.last_heartbeat = datetime.now(timezone.utc)
        
        # Component factories (will be populated by dependency injection)
        self.connector_factory: Optional[ConnectorFactory] = None
        self.classifier_factory: Optional[ClassifierFactory] = None
        
        # Performance metrics
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.total_processing_time = 0.0

        self.logger.log_component_lifecycle("Worker", "INITIALIZED", worker_id=self.worker_id)

    def set_factories(self, connector_factory: 'ConnectorFactory', classifier_factory: 'ClassifierFactory'):
        """Inject the factory dependencies after construction."""
        self.connector_factory = connector_factory
        self.classifier_factory = classifier_factory

    def start(self):
        """Start the worker main loop."""
        if not self.connector_factory or not self.classifier_factory:
            raise RuntimeError("Factories must be set before starting worker")
            
        self.logger.log_component_lifecycle("Worker", "STARTING", worker_id=self.worker_id)
        self.status = WorkerStatus.IDLE
        
        try:
            self._main_loop()
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "worker_main_loop",
                operation="worker_startup",
                worker_id=self.worker_id
            )
            self.logger.error("Worker failed to start", error_id=error.error_id)
            self.status = WorkerStatus.ERROR
            raise
        finally:
            self.status = WorkerStatus.STOPPED
            self.logger.log_component_lifecycle("Worker", "STOPPED", worker_id=self.worker_id)

    def shutdown(self):
        """Signal the worker to shutdown gracefully."""
        self.logger.log_component_lifecycle("Worker", "SHUTDOWN_REQUESTED", worker_id=self.worker_id)
        self.shutdown_event.set()

    def get_status(self) -> Dict[str, Any]:
        """Get current worker status and metrics."""
        return {
            "worker_id": self.worker_id,
            "status": self.status.value,
            "current_task_id": self.current_task.header.task_id if self.current_task else None,
            "tasks_completed": self.tasks_completed,
            "tasks_failed": self.tasks_failed,
            "avg_processing_time": self.total_processing_time / max(1, self.tasks_completed + self.tasks_failed),
            "last_heartbeat": self.last_heartbeat.isoformat()
        }

    def _main_loop(self):
        """Main worker event loop."""
        self.logger.info(f"Worker {self.worker_id} started main loop")
        
        while not self.shutdown_event.is_set():
            try:
                # Get task from orchestrator
                work_packet = self._get_task_from_orchestrator()
                
                if work_packet:
                    self._process_task(work_packet)
                else:
                    # No work available, wait before retrying
                    time.sleep(1)
                    
            except Exception as e:
                error = self.error_handler.handle_error(
                    e, "worker_main_loop",
                    operation="main_loop_iteration",
                    worker_id=self.worker_id
                )
                self.logger.error("Error in worker main loop", error_id=error.error_id)
                
                # Back off on repeated failures
                time.sleep(5)

    def _get_task_from_orchestrator(self) -> Optional[WorkPacket]:
        """Get next task from orchestrator (mode-dependent)."""
        try:
            if self.is_single_process_mode:
                # Direct method call to orchestrator
                work_packet_dict = self.orchestrator.get_task(self.worker_id)
                if work_packet_dict:
                    return WorkPacket(**work_packet_dict)
            else:
                # HTTP API call to orchestrator
                response = requests.post(
                    f"{self.orchestrator_url}/api/get_task",
                    json={"worker_id": self.worker_id},
                    timeout=self.settings.orchestrator.long_poll_timeout_seconds
                )
                response.raise_for_status()
                
                work_packet_dict = response.json()
                if work_packet_dict:
                    return WorkPacket(**work_packet_dict)
            
            return None
            
        except requests.exceptions.RequestException as e:
            error = self.error_handler.handle_error(
                e, "get_task_from_orchestrator",
                operation="orchestrator_communication",
                worker_id=self.worker_id
            )
            self.logger.warning("Failed to get task from orchestrator", error_id=error.error_id)
            return None
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "get_task_from_orchestrator", 
                operation="task_retrieval",
                worker_id=self.worker_id
            )
            self.logger.error("Unexpected error getting task", error_id=error.error_id)
            return None

    def _process_task(self, work_packet: WorkPacket):
        """Process a single work packet."""
        task_id = work_packet.header.task_id
        self.current_task = work_packet
        self.status = WorkerStatus.PROCESSING_TASK
        
        start_time = time.time()
        
        try:
            self.logger.log_task_assignment(
                task_id, 
                self.worker_id, 
                work_packet.header.trace_id
            )
            
            # Dispatch to appropriate processor based on task type
            if work_packet.payload.task_type == TaskType.DISCOVERY_ENUMERATE:
                self._process_discovery_enumerate(work_packet)
            elif work_packet.payload.task_type == TaskType.DISCOVERY_GET_DETAILS:
                self._process_discovery_get_details(work_packet)
            elif work_packet.payload.task_type == TaskType.CLASSIFICATION:
                self._process_classification(work_packet)
            elif work_packet.payload.task_type == TaskType.DELTA_CALCULATE:
                self._process_delta_calculate(work_packet)
            else:
                raise ValueError(f"Unknown task type: {work_packet.payload.task_type}")
                
            # Report successful completion
            self._report_task_completion(task_id, "COMPLETED", {})
            self.tasks_completed += 1
            
            processing_time = time.time() - start_time
            self.total_processing_time += processing_time
            
            self.logger.log_task_completion(task_id, "COMPLETED")
            
        except Exception as e:
            # Handle task failure
            error = self.error_handler.handle_error(
                e, "process_task",
                operation="task_processing", 
                task_id=task_id,
                worker_id=self.worker_id
            )
            
            self._report_task_completion(task_id, "FAILED", {"error_id": error.error_id})
            self.tasks_failed += 1
            
            self.logger.log_task_completion(task_id, "FAILED")
            
        finally:
            self.current_task = None
            self.status = WorkerStatus.IDLE

    def _process_discovery_enumerate(self, work_packet: WorkPacket):
        """Process DISCOVERY_ENUMERATE task."""
        payload = work_packet.payload
        
        # Get appropriate connector
        connector = self.connector_factory.get_connector(payload.datasource_id)
        
        # Process enumeration with progress reporting
        total_objects = 0
        batch_count = 0
        
        for batch in connector.enumerate_objects(work_packet):
            batch_count += 1
            batch_size = len(batch)
            total_objects += batch_size
            
            # Report progress to orchestrator
            progress = TaskOutputRecord(
                output_type="DISCOVERED_OBJECTS",
                output_payload={
                    "staging_table": payload.staging_table_name,
                    "batch_id": f"batch_{batch_count}",
                    "count": batch_size
                }
            )
            self._report_task_progress(work_packet.header.task_id, progress)
            
            # Send heartbeat for long-running tasks
            if batch_count % 10 == 0:
                self._send_heartbeat(work_packet.header.task_id)

        self.logger.info(f"Discovery enumeration completed: {total_objects} objects found", 
                        task_id=work_packet.header.task_id, 
                        total_objects=total_objects)

    def _process_discovery_get_details(self, work_packet: WorkPacket):
        """Process DISCOVERY_GET_DETAILS task.""" 
        payload = work_packet.payload
        
        # Get appropriate connector
        connector = self.connector_factory.get_connector(payload.datasource_id)
        
        # Process detailed metadata retrieval
        metadata_results = connector.get_object_details(work_packet)
        
        # Report completion
        progress = TaskOutputRecord(
            output_type="OBJECT_DETAILS_FETCHED",
            output_payload={
                "object_ids": payload.object_ids,
                "success_count": len(metadata_results)
            }
        )
        self._report_task_progress(work_packet.header.task_id, progress)

    def _process_classification_ToBeRemoved(self, work_packet: WorkPacket):
        """Process CLASSIFICATION task."""
        payload = work_packet.payload
        
        # Get appropriate connector and classifier
        connector = self.connector_factory.get_connector(payload.datasource_id)
        classifier = self.classifier_factory.get_classification_engine(payload.classifier_template_id)
        
        # Process content classification
        total_findings = 0
        objects_processed = 0
        
        for content_batch in connector.get_object_content(work_packet):
            object_id = content_batch.get("object_id")
            content = content_batch.get("content")
            
            if object_id and content:
                # Classify content
                findings = classifier.classify_content(object_id, content, work_packet)
                total_findings += len(findings)
                objects_processed += 1
                
                # Send heartbeat periodically
                if objects_processed % 10 == 0:
                    self._send_heartbeat(work_packet.header.task_id)

        self.logger.info(f"Classification completed: {objects_processed} objects, {total_findings} findings",
                        task_id=work_packet.header.task_id,
                        objects_processed=objects_processed,
                        total_findings=total_findings)


    def _process_classification(self, work_packet: WorkPacket):
        """Process CLASSIFICATION task with interface detection."""
        payload = work_packet.payload
        
        # Get appropriate connector and classifier
        connector = self.connector_factory.get_connector(payload.datasource_id)
        classifier = self.classifier_factory.get_classification_engine(payload.classifier_template_id)
        
        # Detect connector type and route appropriately
        if isinstance(connector, IFileDataSourceConnector):
            self._process_file_based_classification(connector, classifier, work_packet)
        elif isinstance(connector, IDatabaseDataSourceConnector):
            self._process_database_classification(connector, classifier, work_packet)
        else:
            # Fallback to legacy behavior for existing connectors
            self._process_database_classification(connector, classifier, work_packet)

    def _process_file_based_classification(self, connector: IFileDataSourceConnector, 
                                         classifier: IClassificationEngine, work_packet: WorkPacket):
        """Handle file-based connectors with ContentComponent interface."""
        total_findings = 0
        components_processed = 0
        
        # Process ContentComponent objects
        for content_component in connector.get_object_content(work_packet):
            # Route component to appropriate classification method
            findings = self._classify_content_component(content_component, classifier, work_packet)
            total_findings += len(findings)
            components_processed += 1
            
            # Send heartbeat periodically
            if components_processed % 10 == 0:
                self._send_heartbeat(work_packet.header.task_id)

        self.logger.info(f"File-based classification completed",
                        task_id=work_packet.header.task_id,
                        components_processed=components_processed,
                        total_findings=total_findings)

    def _process_database_classification(self, connector: IDatabaseDataSourceConnector,
                                       classifier: IClassificationEngine, work_packet: WorkPacket):
        """Handle database connectors with existing dict interface (UNCHANGED)."""
        total_findings = 0
        objects_processed = 0
        
        # Process dict objects (existing logic)
        for content_batch in connector.get_object_content(work_packet):
            object_id = content_batch.get("object_id")
            content = content_batch.get("content")
            
            if object_id and content:
                # Classify content using existing method
                findings = classifier.classify_content(object_id, content, work_packet)
                total_findings += len(findings)
                objects_processed += 1
                
                # Send heartbeat periodically
                if objects_processed % 10 == 0:
                    self._send_heartbeat(work_packet.header.task_id)

        self.logger.info(f"Database classification completed",
                        task_id=work_packet.header.task_id,
                        objects_processed=objects_processed,
                        total_findings=total_findings)



    def _process_delta_calculate(self, work_packet: WorkPacket):
        """Process DELTA_CALCULATE task."""
        # This is typically a database-only operation
        # In a real implementation, this would execute SQL comparisons
        
        self.logger.info("Delta calculation task processed", 
                        task_id=work_packet.header.task_id)
        
        # Simulate delta calculation work
        time.sleep(2)

    def _report_task_progress(self, task_id: int, progress_record: TaskOutputRecord):
        """Report intermediate progress to orchestrator."""
        self.status = WorkerStatus.REPORTING_PROGRESS
        
        try:
            if self.is_single_process_mode:
                self.orchestrator.update_task_progress(
                    task_id, 
                    progress_record.dict(), 
                    {"worker_id": self.worker_id}
                )
            else:
                response = requests.post(
                    f"{self.orchestrator_url}/api/update_task_progress",
                    json={
                        "task_id": task_id,
                        "progress_record": progress_record.dict(),
                        "worker_id": self.worker_id
                    },
                    timeout=30
                )
                response.raise_for_status()
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "report_task_progress",
                operation="progress_reporting",
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.warning("Failed to report task progress", error_id=error.error_id)
        finally:
            self.status = WorkerStatus.PROCESSING_TASK

    def _report_task_completion(self, task_id: int, status: str, result_payload: Dict[str, Any]):
        """Report task completion to orchestrator."""
        try:
            if self.is_single_process_mode:
                self.orchestrator.report_task_result(
                    task_id, 
                    status, 
                    status == "FAILED",  # is_retryable
                    result_payload,
                    {"worker_id": self.worker_id}
                )
            else:
                response = requests.post(
                    f"{self.orchestrator_url}/api/complete_task",
                    json={
                        "task_id": task_id,
                        "status": status,
                        "final_summary": result_payload,
                        "worker_id": self.worker_id
                    },
                    timeout=30
                )
                response.raise_for_status()
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "report_task_completion",
                operation="completion_reporting",
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.error("Failed to report task completion", error_id=error.error_id)

    def _send_heartbeat(self, task_id: int):
        """Send heartbeat to orchestrator for long-running tasks."""
        self.last_heartbeat = datetime.now(timezone.utc)
        
        try:
            if self.is_single_process_mode:
                self.orchestrator.report_heartbeat(task_id, {"worker_id": self.worker_id})
            else:
                response = requests.post(
                    f"{self.orchestrator_url}/api/report_heartbeat",
                    json={"task_id": task_id, "worker_id": self.worker_id},
                    timeout=10
                )
                response.raise_for_status()
                
            self.logger.log_heartbeat(task_id, self.worker_id)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "send_heartbeat",
                operation="heartbeat_reporting", 
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.warning("Failed to send heartbeat", error_id=error.error_id)


    def _classify_content_component(self, component: ContentComponent, 
                                   classifier: IClassificationEngine, work_packet: WorkPacket) -> List[PIIFinding]:
        """Route ContentComponent to appropriate classification method."""
        try:
            if component.component_type == "table":
                # Route to structured data classification
                return self._classify_table_component(component, classifier, work_packet)
            elif component.component_type in ["text", "image_ocr", "table_fallback", "archive_member"]:
                # Route to unstructured text classification
                return classifier.classify_content(component.object_id, component.content, work_packet)
            elif component.component_type in ["extraction_error", "unsupported_format", "file_too_large"]:
                # No classification needed for error components
                return []
            else:
                self.logger.warning("Unknown component type for classification",
                                   component_type=component.component_type,
                                   component_id=component.component_id)
                return []
        except Exception as e:
            self.logger.error("Component classification failed",
                             component_id=component.component_id,
                             error=str(e))
            return []

    def _classify_table_component(self, component: ContentComponent, 
                                 classifier: IClassificationEngine, work_packet: WorkPacket) -> List[PIIFinding]:
        """Handle table component classification with structured data."""
        # Convert table schema to rows and classify each row
        # Implementation depends on classification engine capabilities
        # For now, fallback to text content classification
        return classifier.classify_content(component.object_id, component.content, work_packet)


# =============================================================================
# Factory Classes for Dependency Injection
# =============================================================================

class ConnectorFactory_ToBeRemoved:
    """Factory for creating connector instances based on datasource configuration."""
    
    def __init__(self, logger: SystemLogger, error_handler: ErrorHandler):
        self.logger = logger
        self.error_handler = error_handler
        self.connector_registry: Dict[str, IDataSourceConnector] = {}
        
    def register_connector(self, datasource_id: str, connector: IDataSourceConnector):
        """Register a connector instance for a specific datasource."""
        self.connector_registry[datasource_id] = connector
        self.logger.info(f"Registered connector for datasource {datasource_id}")
        
    def get_connector(self, datasource_id: str) -> IDataSourceConnector:
        """Get connector instance for datasource."""
        if datasource_id not in self.connector_registry:
            raise ValueError(f"No connector registered for datasource: {datasource_id}")
        return self.connector_registry[datasource_id]



class ConnectorFactory:
    """Factory for creating connector instances with interface detection."""
    
    def __init__(self, logger: SystemLogger, error_handler: ErrorHandler):
        self.logger = logger
        self.error_handler = error_handler
        self.connector_registry: Dict[str, Union[IFileDataSourceConnector, IDatabaseDataSourceConnector]] = {}
        
    def register_connector(self, datasource_id: str, 
                          connector: Union[IFileDataSourceConnector, IDatabaseDataSourceConnector]):
        """Register a connector instance for a specific datasource."""
        self.connector_registry[datasource_id] = connector
        
        # Log interface type
        interface_type = "File-based" if isinstance(connector, IFileDataSourceConnector) else "Database"
        self.logger.info(f"Registered {interface_type} connector for datasource {datasource_id}")
        
    def get_connector(self, datasource_id: str) -> Union[IFileDataSourceConnector, IDatabaseDataSourceConnector]:
        """Get connector instance for datasource."""
        if datasource_id not in self.connector_registry:
            raise ValueError(f"No connector registered for datasource: {datasource_id}")
        return self.connector_registry[datasource_id]


class ClassifierFactory:
    """Factory for creating classification engine instances based on template configuration."""
    
    def __init__(self, logger: SystemLogger, error_handler: ErrorHandler):
        self.logger = logger
        self.error_handler = error_handler
        self.engine_registry: Dict[str, IClassificationEngine] = {}
        
    def register_classification_engine(self, template_id: str, engine: IClassificationEngine):
        """Register a classification engine for a specific template."""
        self.engine_registry[template_id] = engine
        self.logger.info(f"Registered classification engine for template {template_id}")
        
    def get_classification_engine(self, template_id: str) -> IClassificationEngine:
        """Get classification engine instance for template."""
        if template_id not in self.engine_registry:
            raise ValueError(f"No classification engine registered for template: {template_id}")
        return self.engine_registry[template_id]


# =============================================================================
# Worker Factory and Management
# =============================================================================

def create_worker(settings: SystemConfig, 
                 logger: SystemLogger, 
                 error_handler: ErrorHandler,
                 connector_factory: ConnectorFactory,
                 classifier_factory: ClassifierFactory,
                 **kwargs) -> Worker:
    """Factory function to create a properly configured worker instance."""
    
    worker = Worker(settings, logger, error_handler, **kwargs)
    worker.set_factories(connector_factory, classifier_factory)
    
    return worker


def create_worker_pool(count: int,
                      settings: SystemConfig,
                      logger: SystemLogger, 
                      error_handler: ErrorHandler,
                      connector_factory: ConnectorFactory,
                      classifier_factory: ClassifierFactory,
                      **worker_kwargs) -> List[Worker]:
    """Create a pool of worker instances."""
    
    workers = []
    for i in range(count):
        worker_id = f"worker-pool-{i+1}"
        worker = create_worker(
            settings, logger, error_handler,
            connector_factory, classifier_factory,
            worker_id=worker_id,
            **worker_kwargs
        )
        workers.append(worker)
    
    return workers