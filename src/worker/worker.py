# src/worker/worker.py
"""
The Worker implementation that supports the defined interfaces for
orchestrator communication, connector usage, and classification engine integration.

This worker implements the contracts defined in:
- WorkerOrchestratorInterface.txt  
- worker_interfaces.py (IDatabaseDataSourceConnector, IFileDataSourceConnector)
- EngineInterface integration for classification
"""

import asyncio
import time
import threading
import requests
import json
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timezone
import traceback
from enum import Enum

from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.errors import ErrorHandler, ClassificationError, NetworkError, ProcessingError
from core.models.models import WorkPacket, TaskType, TaskOutputRecord, ProgressUpdate, ContentComponent, PIIFinding
from core.interfaces.worker_interfaces import (
    IDatabaseDataSourceConnector, 
    IFileDataSourceConnector
)
from classification.engineinterface import EngineInterface
from core.db.database_interface import DatabaseInterface
from core.config.configuration_manager import ConfigurationManager

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
                 db_interface: DatabaseInterface,
                 config_manager: ConfigurationManager,
                 worker_id: Optional[str] = None,
                 orchestrator_url: Optional[str] = None,
                 orchestrator_instance: Optional[Any] = None):
        """
        Initialize worker with configuration and dependencies.
        
        Args:
            settings: System configuration
            logger: System logger instance
            error_handler: Error handler instance
            db_interface: Database interface for storing results
            worker_id: Unique worker identifier
            orchestrator_url: URL for orchestrator API (EKS mode)
            orchestrator_instance: Direct orchestrator reference (single-process mode)
        """
        self.settings = settings
        self.logger = logger
        self.error_handler = error_handler
        self.db_interface = db_interface
        self.config_manager = config_manager
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
        
        # Performance metrics
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.total_processing_time = 0.0

        self.logger.log_component_lifecycle("Worker", "INITIALIZED", worker_id=self.worker_id)

    def set_factories(self, connector_factory: 'ConnectorFactory'):
        """Inject the factory dependencies after construction."""
        self.connector_factory = connector_factory

    def start(self):
        """Start the worker main loop."""
        if not self.connector_factory:
            raise RuntimeError("ConnectorFactory must be set before starting worker")
            
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
                # Run classification in proper async context
                try:
                    asyncio.run(self._process_classification(work_packet))
                except RuntimeError as e:
                    if "cannot be called from a running event loop" in str(e):
                        # Handle nested event loop case  
                        loop = asyncio.get_running_loop()
                        task = loop.create_task(self._process_classification(work_packet))
                        loop.run_until_complete(task)
                    else:
                        raise
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

    async def _process_classification(self, work_packet: WorkPacket):
        """Process CLASSIFICATION task with interface detection."""
        payload = work_packet.payload
        
        # Get appropriate connector
        connector = self.connector_factory.get_connector(payload.datasource_id)
        
        # Create EngineInterface for this specific work packet
        job_context = {
            "job_id": work_packet.header.trace_id,
            "task_id": work_packet.header.task_id,
            "datasource_id": work_packet.payload.datasource_id,
            "trace_id": work_packet.header.trace_id,
            "worker_id": self.worker_id
        }
        
        # Create fresh EngineInterface for this task
        engine_interface = EngineInterface(
            db_interface=self.db_interface,
            config_manager=self.config_manager,
            system_logger=self.logger,
            error_handler=self.error_handler,
            job_context=job_context
        )
        
        # Initialize engine interface for this template
        try:
            engine_interface.initialize_for_template(payload.classifier_template_id)
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "initialize_engine_interface",
                operation="engine_initialization",
                template_id=payload.classifier_template_id,
                trace_id=work_packet.header.trace_id
            )
            self.logger.error("Failed to initialize EngineInterface", error_id=error.error_id)
            raise
        
        # Detect connector type and route appropriately
        if isinstance(connector, IFileDataSourceConnector):
            await self._process_file_based_classification(connector, engine_interface, work_packet)
        elif isinstance(connector, IDatabaseDataSourceConnector):
            await self._process_database_classification(connector, engine_interface, work_packet)
        else:
            # Fallback to database classification for existing connectors
            await self._process_database_classification(connector, engine_interface, work_packet)

    async def _process_file_based_classification(self, connector: IFileDataSourceConnector, 
                                               engine_interface: EngineInterface, work_packet: WorkPacket):
        """Handle file-based connectors with ContentComponent interface."""
        all_findings = []  # Accumulate ALL findings from all components
        components_processed = 0
        total_components = 0
        
        self.logger.info("Starting file-based classification",
                        task_id=work_packet.header.task_id)
        
        # Process ContentComponent objects and accumulate findings
        for content_component in connector.get_object_content(work_packet):
            total_components += 1
            
            try:
                # Get findings for this component
                component_findings = await self._classify_content_component(content_component, engine_interface, work_packet)
                all_findings.extend(component_findings)  # Accumulate findings
                components_processed += 1
                
                self.logger.debug("Component classified",
                                component_id=content_component.component_id,
                                component_type=content_component.component_type,
                                findings_count=len(component_findings))
                
            except Exception as e:
                error = self.error_handler.handle_error(
                    e, f"classify_component_{content_component.component_id}",
                    operation="component_classification",
                    component_type=content_component.component_type,
                    trace_id=work_packet.header.trace_id
                )
                self.logger.warning("Component classification failed", error_id=error.error_id)
                # Continue processing other components
            
            # Send heartbeat periodically
            if components_processed % 10 == 0:
                self._send_heartbeat(work_packet.header.task_id)

        # Convert ALL accumulated findings to database format
        self.logger.info("Converting findings to database format",
                        task_id=work_packet.header.task_id,
                        total_findings=len(all_findings),
                        total_components=total_components)
        
        db_records = engine_interface.convert_findings_to_db_format(
            all_findings=all_findings,
            total_rows_scanned=total_components  # For files, this represents component count
        )
        
        # Store results in database
        await self._store_classification_results(db_records, work_packet)

        self.logger.info("File-based classification completed",
                        task_id=work_packet.header.task_id,
                        components_processed=components_processed,
                        total_components=total_components,
                        total_findings=len(all_findings),
                        db_records_created=len(db_records))

    async def _process_database_classification(self, connector: IDatabaseDataSourceConnector,
                                             engine_interface: EngineInterface, work_packet: WorkPacket):
        """Handle database connectors with existing dict interface."""
        all_findings = []  # Accumulate ALL findings from all objects
        objects_processed = 0
        total_objects = 0
        
        self.logger.info("Starting database classification",
                        task_id=work_packet.header.task_id)
        
        # Process dict objects and accumulate findings
        for content_batch in connector.get_object_content(work_packet):
            total_objects += 1
            object_id = content_batch.get("object_id")
            content = content_batch.get("content")
            
            if object_id and content:
                try:
                    # For database content, treat as document content
                    file_metadata = {
                        "file_path": f"database_object_{object_id}",
                        "file_name": object_id,
                        "field_name": object_id,
                        "extraction_source": "database"
                    }
                    
                    # Get findings for this database object
                    object_findings = await engine_interface.classify_document_content(
                        content=content,
                        file_metadata=file_metadata
                    )
                    
                    all_findings.extend(object_findings)  # Accumulate findings
                    objects_processed += 1
                    
                    self.logger.debug("Database object classified",
                                    object_id=object_id,
                                    findings_count=len(object_findings))
                    
                except Exception as e:
                    error = self.error_handler.handle_error(
                        e, f"classify_database_object_{object_id}",
                        operation="database_object_classification",
                        object_id=object_id,
                        trace_id=work_packet.header.trace_id
                    )
                    self.logger.warning("Database object classification failed", error_id=error.error_id)
                    # Continue processing other objects
                
                # Send heartbeat periodically
                if objects_processed % 10 == 0:
                    self._send_heartbeat(work_packet.header.task_id)

        # Convert ALL accumulated findings to database format
        self.logger.info("Converting findings to database format",
                        task_id=work_packet.header.task_id,
                        total_findings=len(all_findings),
                        total_objects=total_objects)
        
        db_records = engine_interface.convert_findings_to_db_format(
            all_findings=all_findings,
            total_rows_scanned=total_objects  # For database, this represents object count
        )
        
        # Store results in database
        await self._store_classification_results(db_records, work_packet)

        self.logger.info("Database classification completed",
                        task_id=work_packet.header.task_id,
                        objects_processed=objects_processed,
                        total_objects=total_objects,
                        total_findings=len(all_findings),
                        db_records_created=len(db_records))

    async def _classify_content_component(self, component: ContentComponent, 
                                        engine_interface: EngineInterface, work_packet: WorkPacket) -> List[PIIFinding]:
        """Route ContentComponent to appropriate classification method and return accumulated findings."""
        try:
            if component.component_type == "table":
                # Route to row-by-row classification
                return await self._classify_table_component(component, engine_interface, work_packet)
                
            elif component.component_type in ["text", "image_ocr", "table_fallback", "archive_member"]:
                # Route to document content classification
                file_metadata = {
                    "file_path": component.parent_path,
                    "file_name": component.parent_path.split('/')[-1] if component.parent_path else component.component_id,
                    "field_name": component.component_id,
                    "component_type": component.component_type,
                    "extraction_method": component.extraction_method,
                    "extraction_source": "archive_member" if component.is_archive_extraction else "file"
                }
                
                if component.is_archive_extraction:
                    archive_parent = component.parent_path.split('/')[0] if component.parent_path else ""
                    file_metadata["archive_parent"] = archive_parent
                
                return await engine_interface.classify_document_content(
                    content=component.content,
                    file_metadata=file_metadata
                )
                
            elif component.component_type in ["extraction_error", "unsupported_format", "file_too_large", "no_content_extractable"]:
                # No classification needed for error components
                self.logger.debug("Skipping classification for error component",
                                component_id=component.component_id,
                                component_type=component.component_type)
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

    async def _classify_table_component(self, component: ContentComponent, 
                                      engine_interface: EngineInterface, work_packet: WorkPacket) -> List[PIIFinding]:
        """Handle table component: row-by-row classification with accumulation."""
        
        all_row_findings = []  # Accumulate findings from all rows in this table
        
        try:
            # Validate component has required table data
            if not hasattr(component, 'schema') or not component.schema:
                self.logger.warning("Table component missing schema",
                                   component_id=component.component_id)
                return []
            
            table_schema = component.schema
            
            # Handle both possible table data formats
            table_rows = []
            if "rows" in table_schema and isinstance(table_schema["rows"], list):
                # Structured format: data in schema
                table_rows = table_schema["rows"]
            elif component.content:
                # Serialized format: parse from content
                table_rows = self._parse_serialized_table_content(component.content, table_schema)
            
            if not table_rows:
                self.logger.warning("Table component has no rows to process",
                                   component_id=component.component_id)
                return []
            
            headers = table_schema.get("headers", [])
            
            # Build table metadata for classification
            table_metadata = {
                "table_name": component.component_id,
                "columns": {header: {} for header in headers},
                "source_file": component.parent_path,
                "component_type": component.component_type,
                "schema_name": None  # Files don't have schema names
            }
            
            # Classify each row individually and accumulate findings
            for row_index, row_data in enumerate(table_rows):
                try:
                    # Ensure row_data is dict format
                    if not isinstance(row_data, dict):
                        self.logger.warning("Invalid row data format",
                                           component_id=component.component_id,
                                           row_index=row_index,
                                           row_type=type(row_data))
                        continue
                    
                    row_pk = {
                        "row_index": row_index, 
                        "component_id": component.component_id
                    }
                    
                    # Single row classification call
                    row_findings = await engine_interface.classify_database_row(
                        row_data=row_data,
                        row_pk=row_pk,
                        table_metadata=table_metadata
                    )
                    
                    # Add component context to each finding
                    for finding in row_findings:
                        if not hasattr(finding, 'context_data') or finding.context_data is None:
                            finding.context_data = {}
                        finding.context_data.update({
                            "component_id": component.component_id,
                            "component_type": component.component_type,
                            "extraction_method": component.extraction_method,
                            "row_index": row_index,
                            "file_path": component.parent_path,
                            "file_name": component.parent_path.split('/')[-1] if component.parent_path else component.component_id,
                            "field_name": component.component_id  # Use component_id as field_name
                        })
                    
                    # Accumulate row findings
                    all_row_findings.extend(row_findings)
                    
                except Exception as row_error:
                    self.logger.warning("Row classification failed",
                                       component_id=component.component_id,
                                       row_index=row_index,
                                       error=str(row_error))
                    # Continue processing other rows
                    continue
            
            self.logger.info("Table component classification completed",
                            component_id=component.component_id,
                            rows_processed=len(table_rows),
                            findings_found=len(all_row_findings))
            
            return all_row_findings  # Return accumulated findings from all rows
            
        except Exception as e:
            self.logger.error("Table component classification failed",
                             component_id=component.component_id, 
                             error=str(e))
            return []

    def _parse_serialized_table_content(self, content: str, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse serialized table content into row dictionaries."""
        try:
            if not content or not content.strip():
                return []
            
            headers = schema.get("headers", [])
            if not headers:
                return []
            
            # Split content into rows (assuming newline separated)
            content_lines = content.strip().split('\n')
            rows = []
            
            for line in content_lines:
                if not line.strip():
                    continue
                
                # Split by pipe separator (based on specification examples)
                values = [v.strip() for v in line.split('|')]
                
                # Create row dict matching headers
                row_dict = {}
                for i, header in enumerate(headers):
                    row_dict[header] = values[i] if i < len(values) else ""
                
                rows.append(row_dict)
            
            return rows
            
        except Exception as e:
            self.logger.error("Failed to parse serialized table content",
                             error=str(e))
            return []

    async def _store_classification_results(self, db_records: List[Dict[str, Any]], work_packet: WorkPacket):
        """Store classification results in database."""
        if not db_records:
            self.logger.info("No classification results to store",
                            task_id=work_packet.header.task_id)
            return
            
        try:
            # Build job context for database call
            job_context = {
                "job_id": work_packet.header.trace_id,  # Use trace_id as job_id
                "task_id": work_packet.header.task_id,
                "datasource_id": work_packet.payload.datasource_id,
                "trace_id": work_packet.header.trace_id,
                "worker_id": self.worker_id
            }
            
            # Store results using database interface
            self.db_interface.insert_scan_findings(db_records, context=job_context)
            
            self.logger.info("Classification results stored successfully",
                            task_id=work_packet.header.task_id,
                            records_stored=len(db_records))
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "store_classification_results",
                operation="database_insert",
                task_id=work_packet.header.task_id,
                records_count=len(db_records)
            )
            self.logger.error("Failed to store classification results", error_id=error.error_id)
            raise

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


# =============================================================================
# Factory Classes for Dependency Injection
# =============================================================================

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


class EngineFactory:
    """Factory for creating EngineInterface instances."""
    
    def __init__(self, db_interface: DatabaseInterface, config_manager: ConfigurationManager, 
                 logger: SystemLogger, error_handler: ErrorHandler):
        self.db_interface = db_interface
        self.config_manager = config_manager
        self.logger = logger
        self.error_handler = error_handler
        
    def create_engine_interface(self, job_context: Dict[str, Any]) -> EngineInterface:
        """Create a fresh EngineInterface instance for a specific job context."""
        return EngineInterface(
            db_interface=self.db_interface,
            config_manager=self.config_manager,
            system_logger=self.logger,
            error_handler=self.error_handler,
            job_context=job_context
        )


# =============================================================================
# Worker Factory and Management
# =============================================================================

def create_worker(settings: SystemConfig, 
                 logger: SystemLogger, 
                 error_handler: ErrorHandler,
                 db_interface: DatabaseInterface,
                 config_manager: ConfigurationManager,
                 connector_factory: ConnectorFactory,
                 **kwargs) -> Worker:
    """Factory function to create a properly configured worker instance."""
    
    worker = Worker(settings, logger, error_handler, db_interface, config_manager, **kwargs)
    worker.set_factories(connector_factory)
    
    return worker


def create_worker_pool(count: int,
                      settings: SystemConfig,
                      logger: SystemLogger, 
                      error_handler: ErrorHandler,
                      db_interface: DatabaseInterface,
                      config_manager: ConfigurationManager,
                      connector_factory: ConnectorFactory,
                      **worker_kwargs) -> List[Worker]:
    """Create a pool of worker instances."""
    
    workers = []
    for i in range(count):
        worker_id = f"worker-pool-{i+1}"
        worker = create_worker(
            settings, logger, error_handler, db_interface, config_manager,
            connector_factory,
            worker_id=worker_id,
            **worker_kwargs
        )
        workers.append(worker)
    
    return workers