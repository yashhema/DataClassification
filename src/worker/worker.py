# src/worker/worker.py
"""
The Worker implementation that supports the defined interfaces for
orchestrator communication, connector usage, and classification engine integration.

Task 9 Complete: Full async integration with AsyncIterator connector support
and async database operations.

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

from core.interfaces.worker_interfaces import (
    IDatabaseDataSourceConnector, 
    IFileDataSourceConnector
)
from classification.engineinterface import EngineInterface
from core.db.database_interface import DatabaseInterface
from core.config.configuration_manager import ConfigurationManager
from core.config.configuration_manager import ClassificationConfidenceConfig
from core.models.models import (
    TaskType, WorkPacket, ProgressUpdate, ContentComponent, PIIFinding,
    TaskOutputRecord,
    PolicySelectorPlanPayload,
    PolicySelectorExecutePayload,
    PolicyActionExecutePayload,
    ObjectToProcess,PolicyCommitPlanPayload,PolicyEnrichmentPayload,PolicyReconcilePayload,
    RemediationResult
)


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
    Full async architecture with AsyncIterator connector support.
    """

    def __init__(self, 
                 worker_id: str,
                 settings: SystemConfig, 
                 db_interface: DatabaseInterface,
                 connector_factory: 'ConnectorFactory',
                 logger: SystemLogger, 
                 error_handler: ErrorHandler,
                 orchestrator_url: Optional[str] = None,
                 orchestrator_instance: Optional[Any] = None):
        """
        Initialize worker with configuration and dependencies.
        
        Args:
            worker_id: Unique worker identifier
            settings: System configuration
            db_interface: Database interface for storing results
            connector_factory: Factory for creating connectors
            logger: System logger instance
            error_handler: Error handler instance
            orchestrator_url: URL for orchestrator API (EKS mode)
            orchestrator_instance: Direct orchestrator reference (single-process mode)
        """
        self.worker_id = worker_id
        self.settings = settings
        self.db_interface = db_interface
        self.connector_factory = connector_factory
        self.logger = logger
        self.error_handler = error_handler
        self.search_provider = create_search_provider(settings, db_interface)
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
        self.shutdown_event = asyncio.Event()
        self.last_heartbeat = datetime.now(timezone.utc)
        
        # Performance metrics
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.total_processing_time = 0.0

        self.logger.log_component_lifecycle("Worker", "INITIALIZED", worker_id=self.worker_id)




    # It's good practice to have a small helper for consistent logging context
    def job_context(work_packet: WorkPacket) -> Dict[str, Any]:
        """Extracts a consistent context dictionary for logging."""
        return {
            "job_id": work_packet.header.job_id,
            "task_id": work_packet.header.task_id,
            "trace_id": work_packet.header.trace_id
        }



    async def _process_policy_selector_plan_async(self, work_packet: WorkPacket):
        """
        Executes the first step of a Policy Job. It determines the total number of
        objects that match the policy and creates a blueprint for the Pipeliner.
        """
        payload: PolicySelectorPlanPayload = work_packet.payload
        context = job_context(work_packet)
        self.logger.info(f"Starting POLICY_SELECTOR_PLAN for plan_id: {payload.plan_id}", **context)

        try:
            # Use the search provider to get the total count from the configured backend (SQL or Elasticsearch)
            total_objects = await self.search_provider.get_object_count(payload.policy_config.selection_criteria.definition)
            self.logger.info(f"Plan '{payload.plan_id}' matched {total_objects} objects.", **context)

            # Create the blueprint for the Pipeliner to fan out the selection tasks.
            blueprint = {
                "plan_id": payload.plan_id,
                "total_objects": total_objects,
                "selection_batch_size": 10000,  # This should be read from config
                "query_definition": payload.policy_config.selection_criteria.definition.dict(),
                "action_definition": payload.policy_config.action_definition.dict()
            }

            await self._report_task_progress_async(
                work_packet.header.task_id,
                TaskOutputRecord(output_type="SELECTION_PLAN_CREATED", output_payload=blueprint)
            )
        except Exception as e:
            self.error_handler.handle_error(e, "policy_selector_plan", **context)
            raise

    async def _process_policy_selector_execute_async(self, work_packet: WorkPacket):
        """
        Executes a paginated query against the search backend and writes the
        results (a batch of objects) to the RemediationLedger.
        """
        payload: PolicySelectorExecutePayload = work_packet.payload
        context = job_context(work_packet)
        self.logger.info(f"Starting POLICY_SELECTOR_EXECUTE for plan_id: {payload.plan_id}, offset: {payload.pagination.offset}", **context)

        try:
            # Use the search provider to get a specific page of results.
            object_results = await self.search_provider.get_object_page(payload.query, payload.pagination)

            if object_results:
                # Create a single "bin" for this batch of results.
                bin_id = f"bin_{payload.pagination.offset}"
                ledger_bin = {
                    "plan_id": payload.plan_id,
                    "bin_id": bin_id,
                    "ObjectIDs": [res["ObjectID"] for res in object_results],
                    "ObjectPaths": [res["ObjectPath"] for res in object_results],
                    "Status": LedgerStatus.PLANNED
                }
                # Insert the bin into the ledger in a single bulk operation.
                await self.db.insert_remediation_ledger_bins([ledger_bin], context=context)
                self.logger.info(f"Wrote {len(object_results)} objects to ledger bin '{bin_id}'.", **context)
            else:
                self.logger.info("Selector task found no objects for its page.", **context)

        except Exception as e:
            self.error_handler.handle_error(e, "policy_selector_execute", **context)
            raise

    async def _process_policy_action_plan_async(self, work_packet: WorkPacket):
        """
        A lightweight task that runs after selection is complete. It verifies the
        plan and creates the blueprint for the Pipeliner to fan out action tasks.
        """
        payload = work_packet.payload
        plan_id = payload.get("plan_id")
        context = job_context(work_packet)
        self.logger.info(f"Starting POLICY_ACTION_PLAN for plan_id: {plan_id}", **context)

        # This task signals the Pipeliner that the selection phase is 100% complete
        # and provides the final, verified plan information.
        action_blueprint = {
            "plan_id": plan_id,
            "action_definition": payload.get("action_definition")
        }

        await self._report_task_progress_async(
            work_packet.header.task_id,
            TaskOutputRecord(output_type="ACTION_PLAN_CREATED", output_payload=action_blueprint)
        )


    async def _process_policy_action_execute_async(self, work_packet: WorkPacket):
        """
        Executes a final remediation action (Move, Delete, Tag, etc.) for a single
        "bin" of objects and then creates the output record for the streamlined
        reconciliation step.
        """
        payload: PolicyActionExecutePayload = work_packet.payload
        context = job_context(work_packet)
        self.logger.info(f"Starting POLICY_ACTION_EXECUTE for bin: {payload.bin_id}", **context)

        try:
            # 1. Gracefully handle empty bins of work
            if not payload.objects_to_process:
                self.logger.warning(f"Action task for bin '{payload.bin_id}' received no objects to process.", **context)
                await self.db.update_remediation_ledger_bin_status(
                    plan_id=payload.plan_id,
                    bin_id=payload.bin_id,
                    status=LedgerStatus.ACTION_COMPLETED,
                    result_details={"message": "Bin was empty, no action taken."}
                )
                return

            action = payload.action
            remediation_result: RemediationResult = None

            # 2. Dispatch to the correct component based on action type (External vs. Internal)
            if action.action_type in [ActionType.MOVE, ActionType.DELETE, ActionType.ENCRYPT, ActionType.MIP]:
                # --- EXTERNAL (Connector-Bound) ACTION ---
                first_object_id = payload.objects_to_process[0].ObjectID
                datasource_id = first_object_id.split(':')[0]
                connector = self.connector_factory.get_connector(datasource_id)
                object_paths = [obj.ObjectPath for obj in payload.objects_to_process]
                
                if action.action_type == ActionType.MOVE:
                    remediation_result = await connector.move_objects(object_paths, action.destination_directory, action.tombstone_config, context)
                elif action.action_type == ActionType.DELETE:
                    remediation_result = await connector.delete_objects(object_paths, action.tombstone_config, context)
                else:
                    raise NotImplementedError(f"External action '{action.action_type}' not implemented in worker.")

            elif action.action_type == ActionType.TAG:
                # --- INTERNAL (Database-Bound) ACTION ---
                object_key_hashes = [hashlib.sha256(obj.ObjectID.encode()).digest() for obj in payload.objects_to_process]
                tag_action_details = {
                    "action_type": "TAG", "tags_added": action.tags_to_add,
                    "timestamp": datetime.now(timezone.utc).isoformat(), "job_id": work_packet.header.job_id
                }
                await self.db.add_enrichment_action_to_catalog(object_key_hashes, tag_action_details, context)
                
                # Since the DB call is atomic, we assume full success if no exception was raised.
                remediation_result = RemediationResult(
                    succeeded_paths=[obj.ObjectPath for obj in payload.objects_to_process],
                    failed_paths=[], success_count=len(payload.objects_to_process), failure_count=0
                )
            else:
                raise NotImplementedError(f"Action type '{action.action_type}' not implemented.")

            # 3. Update the RemediationLedger with the detailed outcome (the audit log)
            final_status = LedgerStatus.ACTION_COMPLETED if remediation_result.failure_count == 0 else LedgerStatus.ACTION_FAILED
            await self.db.update_remediation_ledger_bin_status(
                plan_id=payload.plan_id, bin_id=payload.bin_id,
                status=final_status, result_details=remediation_result.dict()
            )
            self.logger.info(f"Updated ledger for bin '{payload.bin_id}'. Success: {remediation_result.success_count}, Failed: {remediation_result.failure_count}", **context)

            # 4. Create the TaskOutputRecord to trigger the streamlined reconciliation
            updates_for_catalog = []
            if remediation_result.succeeded_paths:
                for path in remediation_result.succeeded_paths:
                    # This helper would generate the consistent hash from the object's core identity
                    object_key_hash = hashlib.sha256(f"{datasource_id}|{path}".encode()).digest()
                    update = {"ObjectKeyHash": object_key_hash}
                    if action.action_type == ActionType.MOVE:
                        update["MovedPath"] = f"{action.destination_directory}/{path.split('/')[-1]}"
                    elif action.action_type == ActionType.DELETE:
                        update["IsAvailable"] = False
                        update["RemovedDate"] = datetime.now(timezone.utc)
                    # Note: TAG actions don't need reconciliation as they modify the master record directly.
                    if action.action_type != ActionType.TAG:
                        updates_for_catalog.append(update)
            
            if updates_for_catalog:
                await self._report_task_progress_async(
                    work_packet.header.task_id,
                    TaskOutputRecord(
                        output_type="METADATA_RECONCILE_UPDATES",
                        output_payload={"updates": updates_for_catalog}
                    )
                )

        except Exception as e:
            # If the entire task fails unexpectedly, mark the whole bin as FAILED in the ledger.
            error = self.error_handler.handle_error(e, "policy_action_execute", **context)
            await self.db.update_remediation_ledger_bin_status(
                plan_id=payload.plan_id, bin_id=payload.bin_id,
                status=LedgerStatus.ACTION_FAILED,
                result_details={"error": f"Task-level failure: {str(e)}", "error_id": error.error_id}
            )
            raise # Re-raise the exception to fail the task in the Orchestrator



    async def _process_policy_commit_plan_async(self, work_packet: WorkPacket):
        """
        A lightweight task that runs after selection is complete. It verifies the
        plan in the ledger and creates the blueprint for the Pipeliner to fan out
        the action tasks.
        """
        payload: PolicyCommitPlanPayload = work_packet.payload
        context = job_context(work_packet)
        self.logger.info(f"Starting POLICY_COMMIT_PLAN for plan_id: {payload.plan_id}", **context)

        try:
            # This task acts as a gatekeeper. It performs a final validation on the ledger.
            # For example, ensuring no bins failed during the selection write phase.
            # Note: We query for FAILED status, which shouldn't happen, but is a safety check.
            failed_bins = await self.db.get_ledger_bins_by_status(payload.plan_id, LedgerStatus.ACTION_FAILED)
            if failed_bins:
                raise RuntimeError(f"Cannot commit plan {payload.plan_id}; {len(failed_bins)} bins failed during creation.")

            # The blueprint tells the Pipeliner to proceed with the action phase.
            action_blueprint = {
                "plan_id": payload.plan_id,
                "action_definition": payload.action_definition.dict()
            }

            await self._report_task_progress_async(
                work_packet.header.task_id,
                TaskOutputRecord(output_type="ACTION_PLAN_CREATED", output_payload=action_blueprint)
            )
            self.logger.info(f"Successfully committed plan '{payload.plan_id}' for action phase.", **context)
        except Exception as e:
            self.error_handler.handle_error(e, "policy_commit_plan", **context)
            raise

# In file: src/worker/worker.py

    async def _process_policy_reconcile_async(self, work_packet: WorkPacket):
        """
        The final step in a policy workflow. Takes a pre-packaged list of updates
        from a completed action task and writes them to the master data catalog.
        """
        payload: PolicyReconcilePayload = work_packet.payload
        context = job_context(work_packet)
        self.logger.info(f"Starting POLICY_RECONCILE for plan_id: {payload.plan_id}", **context)

        try:
            # The worker receives the exact list of updates needed. It does not
            # need to query the ledger or perform any complex logic.
            updates_for_catalog = payload.updates

            if not updates_for_catalog:
                self.logger.warning(f"Reconciliation task received no updates to process for plan '{payload.plan_id}'.", **context)
                return

            # 1. Call the database interface method to perform the bulk update on the
            #    primary data store (DynamoDB or SQL).
            await self.db.reconcile_remediation_updates(
                updates=updates_for_catalog,
                context=context
            )

            self.logger.info(f"Reconciliation for plan '{payload.plan_id}' completed successfully. "
                           f"Applied {len(updates_for_catalog)} updates to the master catalog.", **context)

        except Exception as e:
            # If the database update fails, the task will fail and can be retried.
            self.error_handler.handle_error(e, "policy_reconcile", **context)
            raise



    async def is_healthy(self) -> bool:
        """Check if worker is healthy and ready to process tasks."""
        try:
            # Test database connectivity
            if hasattr(self.db_interface, 'test_connection'):
                await self.db_interface.test_connection()
            
            # Test connector factory
            if not self.connector_factory or len(self.connector_factory.connector_registry) == 0:
                self.logger.warning("No connectors registered in factory", worker_id=self.worker_id)
                return False
            
            self.logger.log_health_check("Worker", True, worker_id=self.worker_id)
            return True
            
        except Exception as e:
            self.logger.log_health_check("Worker", False, worker_id=self.worker_id, error=str(e))
            return False

    async def start_async(self):
        """Start the worker main loop (async version)."""
        if not self.connector_factory:
            raise RuntimeError("ConnectorFactory must be set before starting worker")
            
        self.logger.log_component_lifecycle("Worker", "STARTING", worker_id=self.worker_id)
        self.status = WorkerStatus.IDLE
        
        try:
            await self._main_loop_async()
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

    async def shutdown_async(self):
        """Signal the worker to shutdown gracefully (async version)."""
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

    async def _main_loop_async(self):
        """Main worker event loop (fully async)."""
        self.logger.info(f"Worker {self.worker_id} started async main loop")
        
        while not self.shutdown_event.is_set():
            try:
                # Get task from orchestrator
                work_packet = await self._get_task_from_orchestrator_async()
                
                if work_packet:
                    await self._process_task_async(work_packet)
                else:
                    # No work available, wait before retrying
                    await asyncio.sleep(1)
                    
            except Exception as e:
                error = self.error_handler.handle_error(
                    e, "worker_main_loop",
                    operation="main_loop_iteration",
                    worker_id=self.worker_id
                )
                self.logger.error("Error in worker main loop", error_id=error.error_id)
                
                # Back off on repeated failures
                await asyncio.sleep(5)

    async def _get_task_from_orchestrator_async(self) -> Optional[WorkPacket]:
        """Get next task from orchestrator (mode-dependent, async)."""
        try:
            if self.is_single_process_mode:
                # Direct method call to orchestrator (assume orchestrator has async method)
                work_packet_dict = await self.orchestrator.get_task_async(self.worker_id)
                if work_packet_dict:
                    return WorkPacket(**work_packet_dict)
            else:
                # HTTP API call to orchestrator (use aiohttp for proper async)
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.orchestrator_url}/api/get_task",
                        json={"worker_id": self.worker_id},
                        timeout=aiohttp.ClientTimeout(total=self.settings.orchestrator.long_poll_timeout_seconds)
                    ) as response:
                        response.raise_for_status()
                        work_packet_dict = await response.json()
                        if work_packet_dict:
                            return WorkPacket(**work_packet_dict)
            
            return None
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "get_task_from_orchestrator_async",
                operation="orchestrator_communication",
                worker_id=self.worker_id
            )
            self.logger.warning("Failed to get task from orchestrator", error_id=error.error_id)
            return None

    async def _process_task_async(self, work_packet: WorkPacket):
        """Process a single work packet (fully async)."""
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
                await self._process_discovery_enumerate_async(work_packet)
            elif work_packet.payload.task_type == TaskType.DISCOVERY_GET_DETAILS:
                await self._process_discovery_get_details_async(work_packet)
            elif work_packet.payload.task_type == TaskType.CLASSIFICATION:
                await self._process_classification_async(work_packet)
            elif work_packet.payload.task_type == TaskType.DELTA_CALCULATE:
                await self._process_delta_calculate_async(work_packet)
            elif task_type == TaskType.POLICY_SELECTOR_PLAN:
                await self._process_policy_selector_plan_async(work_packet)
            elif task_type == TaskType.POLICY_SELECTOR_EXECUTE:
                await self._process_policy_selector_execute_async(work_packet)
            elif task_type == TaskType.POLICY_ACTION_PLAN: # Assuming this new task type is added
                await self._process_policy_action_plan_async(work_packet)
            elif task_type == TaskType.POLICY_ACTION_EXECUTE:
                await self._process_policy_action_execute_async(work_packet)



            else:
                raise ValueError(f"Unknown task type: {work_packet.payload.task_type}")
                
            # Report successful completion
            await self._report_task_completion_async(task_id, "COMPLETED", {})
            self.tasks_completed += 1
            
            processing_time = time.time() - start_time
            self.total_processing_time += processing_time
            
            self.logger.log_task_completion(task_id, "COMPLETED")
            
        except Exception as e:
            # Handle task failure
            error = self.error_handler.handle_error(
                e, "process_task_async",
                operation="task_processing", 
                task_id=task_id,
                worker_id=self.worker_id
            )
            
            await self._report_task_completion_async(task_id, "FAILED", {"error_id": error.error_id})
            self.tasks_failed += 1
            
            self.logger.log_task_completion(task_id, "FAILED")
            
        finally:
            self.current_task = None
            self.status = WorkerStatus.IDLE

    async def _process_discovery_enumerate_async(self, work_packet: WorkPacket):
        """Process DISCOVERY_ENUMERATE task (async)."""
        payload = work_packet.payload
        
        # Get appropriate connector
        connector = self.connector_factory.get_connector(payload.datasource_id)
        
        # Process enumeration with progress reporting (async iteration)
        total_objects = 0
        batch_count = 0
        
        async for batch in connector.enumerate_objects(work_packet):
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
            await self._report_task_progress_async(work_packet.header.task_id, progress)
            
            # Send heartbeat for long-running tasks
            if batch_count % 10 == 0:
                await self._send_heartbeat_async(work_packet.header.task_id)

        self.logger.info(f"Discovery enumeration completed: {total_objects} objects found", 
                        task_id=work_packet.header.task_id, 
                        total_objects=total_objects)

    async def _process_discovery_get_details_async(self, work_packet: WorkPacket):
        """Process DISCOVERY_GET_DETAILS task (async).""" 
        payload = work_packet.payload
        
        # Get appropriate connector
        connector = self.connector_factory.get_connector(payload.datasource_id)
        
        # Process detailed metadata retrieval (note: get_object_details is still sync)
        metadata_results = connector.get_object_details(work_packet)
        
        # Report completion
        progress = TaskOutputRecord(
            output_type="OBJECT_DETAILS_FETCHED",
            output_payload={
                "object_ids": payload.object_ids,
                "success_count": len(metadata_results)
            }
        )
        await self._report_task_progress_async(work_packet.header.task_id, progress)

    async def _process_classification_async(self, work_packet: WorkPacket):
        """Process CLASSIFICATION task with async interface detection."""
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
            config_manager=None,  # Will need to be injected properly
            system_logger=self.logger,
            error_handler=self.error_handler,
            job_context=job_context
        )
        
        # Initialize engine interface for this template
        try:
            await engine_interface.initialize_for_template_async(payload.classifier_template_id)
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
            await self._process_file_based_classification_async(connector, engine_interface, work_packet)
        elif isinstance(connector, IDatabaseDataSourceConnector):
            await self._process_database_classification_async(connector, engine_interface, work_packet)
        else:
            # Fallback to database classification for existing connectors
            await self._process_database_classification_async(connector, engine_interface, work_packet)

    async def _process_file_based_classification_async(self, connector: IFileDataSourceConnector, 
                                                     engine_interface: EngineInterface, work_packet: WorkPacket):
        """Handle file-based connectors with ContentComponent interface (async)."""
        all_findings = []  # Accumulate ALL findings from all components
        components_processed = 0
        total_components = 0
        
        self.logger.info("Starting file-based classification",
                        task_id=work_packet.header.task_id)
        
        # Process ContentComponent objects and accumulate findings (ASYNC ITERATION)
        async for content_component in connector.get_object_content(work_packet):
            total_components += 1
            
            try:
                # Get findings for this component
                component_findings = await self._classify_content_component_async(content_component, engine_interface, work_packet)
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
                await self._send_heartbeat_async(work_packet.header.task_id)

        # Convert ALL accumulated findings to database format
        self.logger.info("Converting findings to database format",
                        task_id=work_packet.header.task_id,
                        total_findings=len(all_findings),
                        total_components=total_components)
        
        db_records = await engine_interface.convert_findings_to_db_format_async(
            all_findings=all_findings,
            total_rows_scanned=total_components  # For files, this represents component count
        )
        
        # Store results in database (ASYNC)
        await self._store_classification_results_async(db_records, work_packet)

        self.logger.info("File-based classification completed",
                        task_id=work_packet.header.task_id,
                        components_processed=components_processed,
                        total_components=total_components,
                        total_findings=len(all_findings),
                        db_records_created=len(db_records))

    async def _process_database_classification_async(self, connector: IDatabaseDataSourceConnector,
                                                   engine_interface: EngineInterface, work_packet: WorkPacket):
        """Handle database connectors with existing dict interface (async)."""
        all_findings = []  # Accumulate ALL findings from all objects
        objects_processed = 0
        total_objects = 0
        
        self.logger.info("Starting database classification",
                        task_id=work_packet.header.task_id)
        
        # Process dict objects and accumulate findings (ASYNC ITERATION)
        async for content_batch in connector.get_object_content(work_packet):
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
                    object_findings = await engine_interface.classify_document_content_async(
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
                    await self._send_heartbeat_async(work_packet.header.task_id)

        # Convert ALL accumulated findings to database format
        self.logger.info("Converting findings to database format",
                        task_id=work_packet.header.task_id,
                        total_findings=len(all_findings),
                        total_objects=total_objects)
        
        db_records = await engine_interface.convert_findings_to_db_format_async(
            all_findings=all_findings,
            total_rows_scanned=total_objects  # For database, this represents object count
        )
        
        # Store results in database (ASYNC)
        await self._store_classification_results_async(db_records, work_packet)

        self.logger.info("Database classification completed",
                        task_id=work_packet.header.task_id,
                        objects_processed=objects_processed,
                        total_objects=total_objects,
                        total_findings=len(all_findings),
                        db_records_created=len(db_records))

    async def _classify_content_component_async(self, component: ContentComponent, 
                                              engine_interface: EngineInterface, work_packet: WorkPacket) -> List[PIIFinding]:
        """Route ContentComponent to appropriate classification method and return accumulated findings (async)."""
        try:
            if component.component_type == "table":
                # Route to row-by-row classification
                return await self._classify_table_component_async(component, engine_interface, work_packet)
                
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
                
                return await engine_interface.classify_document_content_async(
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

    async def _classify_table_component_async(self, component: ContentComponent, 
                                            engine_interface: EngineInterface, work_packet: WorkPacket) -> List[PIIFinding]:
        """Handle table component: row-by-row classification with accumulation (async)."""
        
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
                    
                    # Single row classification call (ASYNC)
                    row_findings = await engine_interface.classify_database_row_async(
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

    async def _store_classification_results_async(self, db_records: List[Dict[str, Any]], work_packet: WorkPacket):
        """Store classification results in database (async)."""
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
            
            # Store results using database interface (ASYNC)
            await self.db_interface.insert_scan_findings_async(db_records, context=job_context)
            
            self.logger.info("Classification results stored successfully",
                            task_id=work_packet.header.task_id,
                            records_stored=len(db_records))
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "store_classification_results_async",
                operation="database_insert",
                task_id=work_packet.header.task_id,
                records_count=len(db_records)
            )
            self.logger.error("Failed to store classification results", error_id=error.error_id)
            raise

    async def _process_delta_calculate_async(self, work_packet: WorkPacket):
        """Process DELTA_CALCULATE task (async)."""
        # This is typically a database-only operation
        # In a real implementation, this would execute SQL comparisons
        
        self.logger.info("Delta calculation task processed", 
                        task_id=work_packet.header.task_id)
        
        # Simulate delta calculation work
        await asyncio.sleep(2)

    async def _report_task_progress_async(self, task_id: int, progress_record: TaskOutputRecord):
        """Report intermediate progress to orchestrator (async)."""
        self.status = WorkerStatus.REPORTING_PROGRESS
        
        try:
            if self.is_single_process_mode:
                await self.orchestrator.update_task_progress_async(
                    task_id, 
                    progress_record.dict(), 
                    {"worker_id": self.worker_id}
                )
            else:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.orchestrator_url}/api/update_task_progress",
                        json={
                            "task_id": task_id,
                            "progress_record": progress_record.dict(),
                            "worker_id": self.worker_id
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        response.raise_for_status()
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "report_task_progress_async",
                operation="progress_reporting",
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.warning("Failed to report task progress", error_id=error.error_id)
        finally:
            self.status = WorkerStatus.PROCESSING_TASK

    async def _report_task_completion_async(self, task_id: int, status: str, result_payload: Dict[str, Any]):
        """Report task completion to orchestrator (async)."""
        try:
            if self.is_single_process_mode:
                await self.orchestrator.report_task_result_async(
                    task_id, 
                    status, 
                    status == "FAILED",  # is_retryable
                    result_payload,
                    {"worker_id": self.worker_id}
                )
            else:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.orchestrator_url}/api/complete_task",
                        json={
                            "task_id": task_id,
                            "status": status,
                            "final_summary": result_payload,
                            "worker_id": self.worker_id
                        },
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        response.raise_for_status()
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "report_task_completion_async",
                operation="completion_reporting",
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.error("Failed to report task completion", error_id=error.error_id)

    async def _send_heartbeat_async(self, task_id: int):
        """Send heartbeat to orchestrator for long-running tasks (async)."""
        self.last_heartbeat = datetime.now(timezone.utc)
        
        try:
            if self.is_single_process_mode:
                await self.orchestrator.report_heartbeat_async(task_id, {"worker_id": self.worker_id})
            else:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.orchestrator_url}/api/report_heartbeat",
                        json={"task_id": task_id, "worker_id": self.worker_id},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        response.raise_for_status()
                
            self.logger.log_heartbeat(task_id, self.worker_id)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "send_heartbeat_async",
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


# =============================================================================
# Worker Factory and Management
# =============================================================================

async def create_worker_async(worker_id: str,
                            settings: SystemConfig, 
                            db_interface: DatabaseInterface,
                            connector_factory: ConnectorFactory,
                            logger: SystemLogger, 
                            error_handler: ErrorHandler,
                            **kwargs) -> Worker:
    """Factory function to create a properly configured worker instance (async)."""
    
    worker = Worker(
        worker_id=worker_id,
        settings=settings, 
        db_interface=db_interface,
        connector_factory=connector_factory,
        logger=logger, 
        error_handler=error_handler,
        **kwargs
    )
    
    # Test health before returning
    if not await worker.is_healthy():
        raise RuntimeError(f"Worker {worker_id} failed health check during creation")
    
    return worker


async def create_worker_pool_async(count: int,
                                 settings: SystemConfig,
                                 db_interface: DatabaseInterface,
                                 connector_factory: ConnectorFactory,
                                 logger: SystemLogger, 
                                 error_handler: ErrorHandler,
                                 **worker_kwargs) -> List[Worker]:
    """Create a pool of worker instances (async)."""
    
    workers = []
    for i in range(count):
        worker_id = f"worker-pool-{i+1}"
        worker = await create_worker_async(
            worker_id=worker_id,
            settings=settings, 
            db_interface=db_interface,
            connector_factory=connector_factory,
            logger=logger, 
            error_handler=error_handler,
            **worker_kwargs
        )
        workers.append(worker)
    
    return workers