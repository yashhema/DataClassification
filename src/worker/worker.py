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
import base64
import time
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.errors import ErrorHandler

from core.interfaces.worker_interfaces import (
    IDatabaseDataSourceConnector, 
    IFileDataSourceConnector,IComplianceConnector
)
from classification.engineinterface import EngineInterface
from core.db.database_interface import DatabaseInterface
from core.config.configuration_manager import ConfigurationManager
from core.models.models import (
    TaskType, WorkPacket, ContentComponent, PIIFinding, TaskOutputRecord,DiscoveredObject,
    PolicySelectorPlanPayload,
    PolicySelectorExecutePayload,
    PolicyActionExecutePayload,
    PolicyCommitPlanPayload,
    PolicyReconcilePayload,RemediationResult,ClassificationPayload,SystemProfile
)
from .search_provider import create_search_provider
from core.db_models.remediation_ledger_schema import LedgerStatus
from core.models.models import ActionType
from core.utils.hash_utils import generate_object_key_hash
from worker.kafka_producer import KafkaResultsProducer
from worker.kafka_consumer import KafkaControlConsumer
from worker.local_file_writer import LocalFileWriter
from uuid import uuid4



from worker.metrics_collector import MetricsCollector
from worker.health_monitor import HealthMonitor
from worker.kafka_metrics_producer import KafkaMetricsProducer
from worker.kafka_control_consumer import KafkaControlConsumer
from worker.kafka_response_producer import KafkaResponseProducer
from core.models.kafka_messages import ComponentType

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
    ToDo :
    All kafka things needs to be started and finalized in the main run loop
    Same is for metrices collection
    """

    def __init__(
        self,
        worker_id: str,
        settings: SystemConfig,
        db_interface: DatabaseInterface,
        connector_factory: 'ConnectorFactory',
        logger: SystemLogger,
        config_manager: ConfigurationManager,
        error_handler: ErrorHandler,
        orchestrator_url: Optional[str] = None,
        orchestrator_instance: Optional[Any] = None
    ):
        """
        Initialize worker with all components including Kafka.
        
        MODIFIED: Added Kafka producer/consumer initialization
        PRESERVED: All original worker initialization
        """
        # PRESERVED: Original initialization
        self.worker_id = worker_id
        self.settings = settings
        self.db_interface = db_interface
        self.config_manager = config_manager
        self.connector_factory = connector_factory
        self.logger = logger
        self.error_handler = error_handler
        
        # PRESERVED: Backend abstraction (from G1)
        self.backend = self._initialize_backend(
            orchestrator_url,
            orchestrator_instance
        )
        
        # PRESERVED: Original worker state
        self.status = WorkerStatus.IDLE
        self.current_task: Optional[WorkPacket] = None
        self.last_heartbeat: Optional[datetime] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
        # PRESERVED: Original component references
        self.credential_manager: Optional[CredentialManager] = None
        self.engine_interface: Optional[EngineInterface] = None
        
        # NEW: Kafka components
        self.kafka_producer: Optional[KafkaResultsProducer] = None
        self.kafka_consumer: Optional[KafkaControlConsumer] = None
        
        # NEW: Track current task's local file writer
        self.current_local_file_writer: Optional[LocalFileWriter] = None
        
        # PRESERVED: Performance tracking
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.total_processing_time = 0.0


        self.kafka_metrics_producer = KafkaMetricsProducer(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            topic_name=self.settings.kafka.topics.component_metrics,  # "prod_component_metrics_topic"
            logger=self.logger,
            error_handler=self.error_handler,
            ssl_config=self.settings.kafka.ssl_config
        )

        self.kafka_control_consumer = KafkaControlConsumer(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            topic_name=self.settings.kafka.topics.worker_control,  # "prod_worker_control_topic"
            group_id=f"worker-control-{self.settings.worker.nodegroup}",
            nodegroup=self.settings.worker.nodegroup,
            component_id=self.worker_id,
            component_type=ComponentType.WORKER,
            logger=self.logger,
            error_handler=self.error_handler,
            command_handler=self._handle_control_command,  # Phase 6
            ssl_config=self.settings.kafka.ssl_config
        )

        self.kafka_response_producer = KafkaResponseProducer(
            bootstrap_servers=self.settings.kafka.bootstrap_servers,
            topic_name=self.settings.kafka.topics.worker_response,  # "prod_worker_response_topic"
            logger=self.logger,
            error_handler=self.error_handler,
            ssl_config=self.settings.kafka.ssl_config
        )
        
        self.logger.info(
            f"Worker {worker_id} initialized",
            worker_id=worker_id,
            node_group=settings.system_identity.node_group,
            deployment_model=settings.orchestrator.deployment_model,
            kafka_enabled=settings.kafka.enabled  # NEW
        )


    def _initialize_backend(
        self,
        orchestrator_url: Optional[str],
        orchestrator_instance: Optional[Any]
    ) -> IWorkerBackend:
        """
        Creates appropriate backend implementation based on deployment mode.
        
        Logic:
        1. Check deployment_model config
        2. If SINGLE_PROCESS: create DirectDBBackend
        3. If EKS: create APIBackend with mTLS config
        
        Error Handling:
        - If EKS mode but no orchestrator_url: ConfigurationError
        - If cert files missing: ConfigurationError
        """
        is_single_process = (
            self.settings.orchestrator.deployment_model.upper() == "SINGLE_PROCESS"
        )
        
        if is_single_process:
            if not orchestrator_instance:
                raise ConfigurationError(
                    "orchestrator_instance required for SINGLE_PROCESS mode",
                    ErrorType.CONFIGURATION_INVALID
                )
            
            self.logger.info("Initializing DirectDBBackend (single-process mode)")
            return DirectDBBackend(
                db_interface=self.db_interface,
                orchestrator_instance=orchestrator_instance,
                logger=self.logger,
                error_handler=self.error_handler
            )
        else:
            if not orchestrator_url:
                raise ConfigurationError(
                    "orchestrator_url required for EKS mode",
                    ErrorType.CONFIGURATION_INVALID
                )
            
            # Load mTLS cert paths from config
            mtls_config = self.settings.security.worker_mtls
            
            self.logger.info(
                "Initializing APIBackend (EKS mode)",
                backend_url=orchestrator_url
            )
            return APIBackend(
                backend_api_url=orchestrator_url,
                api_key=mtls_config.api_key,
                cert_path=mtls_config.client_cert_path,
                key_path=mtls_config.client_key_path,
                ca_path=mtls_config.ca_cert_path,
                logger=self.logger,
                error_handler=self.error_handler,
                timeout_seconds=self.settings.worker.api_timeout_seconds
            )

    async def _cleanup_on_shutdown(self):
        """
        Cleanup all components on worker shutdown.
        
        MODIFIED: Added Kafka component cleanup
        PRESERVED: Original deregistration and cleanup
        """
        try:
            # NEW: Stop Kafka consumer first
            if self.kafka_consumer:
                await self.kafka_consumer.stop()
            
            # NEW: Stop Kafka producer (flushes pending messages)
            if self.kafka_producer:
                await self.kafka_producer.stop()
            
            # PRESERVED: Deregister worker from backend
            await self.backend.deregister_worker(
                self.worker_id,
                context={"worker_id": self.worker_id}
            )
            
            # PRESERVED: Cancel heartbeat task if running
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            self.logger.info(
                "Worker cleanup completed",
                worker_id=self.worker_id,
                tasks_completed=self.tasks_completed,
                tasks_failed=self.tasks_failed
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "worker_cleanup",
                worker_id=self.worker_id
            )
            self.logger.error(
                "Error during worker cleanup",
                error_id=error.error_id
            )


    # It's good practice to have a small helper for consistent logging context
    @staticmethod
    def job_context(work_packet: WorkPacket) -> Dict[str, Any]:
        """Extracts a consistent context dictionary for logging."""
        return {
            "job_id": work_packet.header.job_id,
            "task_id": work_packet.header.task_id,
            "trace_id": work_packet.header.trace_id,
            
        }







    async def _process_policy_selector_plan_async(self, work_packet: WorkPacket):
        """
        Executes the first step of a Policy Job. It determines the total number of
        objects that match the policy and creates a blueprint for the Pipeliner.
        """
        payload: PolicySelectorPlanPayload = work_packet.payload
        context = self.job_context(work_packet)
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

            await self._report_task_progress_async(work_packet.header.job_id,
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
        context = self.job_context(work_packet)
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
        context = self.job_context(work_packet)
        self.logger.info(f"Starting POLICY_ACTION_PLAN for plan_id: {plan_id}", **context)

        # This task signals the Pipeliner that the selection phase is 100% complete
        # and provides the final, verified plan information.
        action_blueprint = {
            "plan_id": plan_id,
            "action_definition": payload.get("action_definition")
        }

        await self._report_task_progress_async(work_packet.header.job_id,
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
        context = self.job_context(work_packet)
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
                connector = await self.connector_factory.create_connector(datasource_id)
                object_paths = [obj.object_path for obj in payload.objects_to_process]
                
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
                    succeeded_paths=[obj.object_path for obj in payload.objects_to_process],
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
                await self._report_task_progress_async(work_packet.header.job_id,
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
        context = self.job_context(work_packet)
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
                work_packet.header.job_id,
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
        context = self.job_context(work_packet)
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
            '''
            if not self.connector_factory or len(self.connector_factory.connector_registry) == 0:
                self.logger.warning("No connectors registered in factory", worker_id=self.worker_id)
                return False
            '''
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
        """
        Main worker loop with operational infrastructure.
        
        Flow:
            1. Start all operational components
            2. Register component with backend
            3. Start background loops (heartbeat, metrics)
            4. Main task processing loop
            5. Graceful shutdown on exit
        """
        try:
            # Step 1: Start operational Kafka components
            self.logger.info("Starting operational components")
            await self.kafka_metrics_producer.start()
            await self.kafka_control_consumer.start()
            await self.kafka_response_producer.start()
            
            # Step 2: Start metrics collector and health monitor
            await self.metrics_collector.start()
            await self.health_monitor.start()
            
            # Step 3: Register this component
            await self._register_component()
            
            # Step 4: Start background loops
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._metrics_task = asyncio.create_task(self._metrics_loop())
            
            self.logger.info(
                "Worker fully initialized and running",
                worker_id=self.worker_id,
                nodegroup=self.nodegroup
            )
            
            # Step 5: Main task processing loop
            while not self.shutdown_event.is_set():
                try:
                    # Check if draining
                    if self._is_draining:
                        self.logger.info("Worker is draining, not leasing new tasks")
                        await asyncio.sleep(5)
                        continue
                    
                    
                    # Get task from orchestrator
                    work_packet = await self._get_task_from_orchestrator_async()

                    # Lease and process task ; is it calling queue when in standalone mode is missing
                    #work_packet = await self.backend.lease_task(worker_id=self.worker_id,                     #nodegroup=self.nodegroup)
                    
                    if work_packet:
                        await self._process_task(work_packet)
                    else:
                        await asyncio.sleep(1)  # No work available
                        
                except Exception as e:
                    error = self.error_handler.handle_error(
                        e,
                        context="worker_run_loop"
                    )
                    self.logger.error(
                        "Error in main worker loop",
                        error_id=error.error_id
                    )
                    await asyncio.sleep(5)  # Backoff on error
            
            self.logger.info("Worker shutdown requested, exiting main loop")
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="worker_run",
                worker_id=self.worker_id
            )
            self.logger.critical(
                "Critical error in worker.run()",
                error_id=error.error_id
            )
            raise
            
        finally:
            # Step 6: Graceful shutdown
            self.logger.info("Worker shutting down...")
            
            # Cancel background tasks
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            if self._metrics_task:
                self._metrics_task.cancel()
                try:
                    await self._metrics_task
                except asyncio.CancelledError:
                    pass
            
            # Deregister component
            await self._deregister_component()
            
            # Stop all operational components
            await self.health_monitor.stop()
            await self.metrics_collector.stop()
            await self.kafka_control_consumer.stop()
            await self.kafka_response_producer.stop()
            await self.kafka_metrics_producer.stop()
            
            self.logger.info("Worker shutdown complete")


    async def _register_component(self) -> None:
        """
        Register this Worker component with the backend.
        
        Sends COMPONENT_REGISTERED event to prod_component_metrics_topic.
        Backend consumes this to track active workers.
        """
        try:
            message = ComponentMetricsHealthMessage(
                metadata=MetricsHealthMetadata(
                    event_type=RegistrationEventType.COMPONENT_REGISTERED,
                    component_id=self.worker_id,
                    component_type=self.component_type,
                    nodegroup=self.nodegroup
                ),
                version=self.settings.worker.version,  # e.g., "2.0.0"
                capabilities={
                    "task_types": [
                        "DISCOVERY_ENUMERATE",
                        "CLASSIFICATION",
                        "DATABASE_PROFILE",
                        "ENTITLEMENT_EXTRACT",
                        "BENCHMARK_EXECUTE"
                    ],
                    "max_concurrent_tasks": self.settings.worker.max_concurrent_tasks,
                    "results_destination": self.settings.worker.results_destination
                }
            )
            
            success = await self.kafka_metrics_producer.publish_message(message)
            
            if success:
                self._is_registered = True
                self.logger.info(
                    "Worker component registered",
                    worker_id=self.worker_id,
                    nodegroup=self.nodegroup
                )
            else:
                self.logger.warning("Failed to register component (Kafka unavailable)")
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="worker_register_component",
                worker_id=self.worker_id
            )
            self.logger.error(
                "Failed to register component",
                error_id=error.error_id
            )
    async def _heartbeat_loop(self) -> None:
        """
        Periodic heartbeat loop.
        
        Sends COMPONENT_HEARTBEAT every 30 seconds to prove this worker is alive.
        Backend uses this to detect dead/stuck workers.
        """
        while True:
            try:
                await asyncio.sleep(30)  # Heartbeat interval
                
                # Get current health status
                health_status = await self.health_monitor.get_overall_status()
                health_checks = await self.health_monitor.get_all_check_results()
                
                message = ComponentMetricsHealthMessage(
                    metadata=MetricsHealthMetadata(
                        event_type=RegistrationEventType.COMPONENT_HEARTBEAT,
                        component_id=self.worker_id,
                        component_type=self.component_type,
                        nodegroup=self.nodegroup
                    ),
                    health_status=health_status,
                    health_checks=health_checks
                )
                
                success = await self.kafka_metrics_producer.publish_message(message)
                
                if success:
                    self.logger.debug(
                        "Heartbeat sent",
                        health_status=health_status.value
                    )
                else:
                    self.logger.warning("Failed to send heartbeat (Kafka unavailable)")
                    
            except asyncio.CancelledError:
                self.logger.info("Heartbeat loop cancelled")
                break
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    context="worker_heartbeat_loop",
                    worker_id=self.worker_id
                )
                self.logger.error(
                    "Error in heartbeat loop",
                    error_id=error.error_id
                )
                # Continue loop despite error

    async def _metrics_loop(self) -> None:
        """
        Periodic metrics collection loop.
        
        Sends METRICS_SNAPSHOT every 60 seconds with Prometheus metrics.
        Backend/Prometheus scrapes these for monitoring.
        """
        while True:
            try:
                await asyncio.sleep(60)  # Metrics interval
                
                # Collect all metrics
                metrics = await self.metrics_collector.collect_metrics()
                
                message = ComponentMetricsHealthMessage(
                    metadata=MetricsHealthMetadata(
                        event_type=RegistrationEventType.METRICS_SNAPSHOT,
                        component_id=self.worker_id,
                        component_type=self.component_type,
                        nodegroup=self.nodegroup
                    ),
                    metrics=metrics
                )
                
                success = await self.kafka_metrics_producer.publish_message(message)
                
                if success:
                    self.logger.debug(
                        "Metrics snapshot sent",
                        metric_count=len(metrics)
                    )
                else:
                    self.logger.warning("Failed to send metrics (Kafka unavailable)")
                    
            except asyncio.CancelledError:
                self.logger.info("Metrics loop cancelled")
                break
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    context="worker_metrics_loop",
                    worker_id=self.worker_id
                )
                self.logger.error(
                    "Error in metrics loop",
                    error_id=error.error_id
                )
                # Continue loop despite error

    async def _deregister_component(self) -> None:
        """
        Deregister this Worker component on shutdown.
        
        Sends COMPONENT_DEREGISTERED event to prod_component_metrics_topic.
        Backend uses this to immediately mark worker as offline.
        """
        if not self._is_registered:
            return
        
        try:
            message = ComponentMetricsHealthMessage(
                metadata=MetricsHealthMetadata(
                    event_type=RegistrationEventType.COMPONENT_DEREGISTERED,
                    component_id=self.worker_id,
                    component_type=self.component_type,
                    nodegroup=self.nodegroup
                )
            )
            
            success = await self.kafka_metrics_producer.publish_message(message)
            
            if success:
                self.logger.info("Worker component deregistered")
            else:
                self.logger.warning("Failed to deregister component (Kafka unavailable)")
                
        except Exception as e:
            # Don't fail shutdown on deregistration error
            self.logger.warning(
                f"Error deregistering component: {e}",
                worker_id=self.worker_id
            )


    async def _handle_control_command(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Route control commands to appropriate handlers.
        
        This method is called by KafkaControlConsumer when a command is received.
        
        Args:
            message: WorkerControlMessage from prod_worker_control_topic
        """
        command_type = message.metadata.command_type
        request_id = message.metadata.request_identifier
        
        self.logger.info(
            f"Handling control command: {command_type.value}",
            command_type=command_type.value,
            request_id=request_id,
            issued_by=message.metadata.issued_by
        )
        
        try:
            # Send "Working" response immediately
            await self._send_control_working_response(message)
            
            # Route to appropriate handler
            if command_type == ControlCommandType.CONFIG_UPDATE_AVAILABLE:
                await self._handle_config_update(message)
                
            elif command_type == ControlCommandType.SET_LOG_LEVEL:
                await self._handle_set_log_level(message)
                
            elif command_type == ControlCommandType.DRAIN:
                await self._handle_drain(message)
                
            elif command_type == ControlCommandType.RESUME:
                await self._handle_resume(message)
                
            elif command_type == ControlCommandType.SHUTDOWN:
                await self._handle_shutdown(message)
                
            elif command_type == ControlCommandType.HEARTBEAT_FORCE:
                await self._handle_heartbeat_force(message)
                
            else:
                raise ValueError(f"Unknown command type: {command_type}")
                
            self.logger.info(
                f"Control command completed: {command_type.value}",
                request_id=request_id
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_handle_control_command",
                command_type=command_type.value,
                request_id=request_id
            )
            self.logger.error(
                f"Error handling control command: {command_type.value}",
                error_id=error.error_id,
                request_id=request_id
            )
            await self._send_control_error_response(message, str(e), error.error_id)

    async def _handle_config_update(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle CONFIG_UPDATE_AVAILABLE command.
        
        Pattern: Backend tells worker "config has changed, go fetch it"
        
        Flow:
            1. Call API endpoint to get new config
            2. Validate new config
            3. Apply changes (reload settings)
            4. Send Finished response
        """
        request_id = message.metadata.request_identifier
        
        try:
            # Extract API details from message
            api_endpoint = message.api_endpoint
            api_parameters = message.api_parameters or {}
            
            if not api_endpoint:
                raise ValueError("CONFIG_UPDATE_AVAILABLE missing api_endpoint")
            
            self.logger.info(
                "Fetching updated configuration",
                api_endpoint=api_endpoint,
                request_id=request_id
            )
            
            # Call backend API to get new config
            # This uses your existing backend client
            new_config_data = await self.backend.call_api(
                endpoint=api_endpoint,
                parameters=api_parameters,
                context={"request_id": request_id}
            )
            
            # Validate new config
            # Assuming you have a WorkerConfig model
            try:
                new_config = WorkerConfig(**new_config_data)
            except Exception as e:
                raise ValueError(f"Invalid config data: {e}")
            
            # Store old config for rollback
            old_config = self.settings.worker
            
            try:
                # Apply new configuration
                self.settings.worker = new_config
                
                # Update affected components
                # Example: update logging level if changed
                if new_config.log_level != old_config.log_level:
                    self.logger.set_level(new_config.log_level)
                
                self.logger.info(
                    "Configuration updated successfully",
                    request_id=request_id,
                    changes={
                        "log_level": f"{old_config.log_level} → {new_config.log_level}",
                        "results_destination": f"{old_config.results_destination} → {new_config.results_destination}"
                    }
                )
                
                # Send success response
                await self._send_control_finished_response(
                    message,
                    response_data={
                        "config_updated": True,
                        "previous_version": old_config.version,
                        "new_version": new_config.version
                    }
                )
                
            except Exception as e:
                # Rollback on failure
                self.settings.worker = old_config
                raise
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_handle_config_update",
                request_id=request_id
            )
            self.logger.error(
                "Failed to update configuration",
                error_id=error.error_id,
                request_id=request_id
            )
            await self._send_control_error_response(message, str(e), error.error_id)
    async def _handle_set_log_level(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle SET_LOG_LEVEL command.
        
        Changes worker's logging level dynamically.
        Useful for debugging without restart.
        """
        request_id = message.metadata.request_identifier
        
        try:
            # Extract new log level from parameters
            new_level = message.parameters.get("log_level")
            
            if not new_level:
                raise ValueError("SET_LOG_LEVEL missing 'log_level' parameter")
            
            # Validate log level
            valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            if new_level.upper() not in valid_levels:
                raise ValueError(f"Invalid log level: {new_level}. Must be one of {valid_levels}")
            
            old_level = self.logger.get_level()
            
            # Set new log level
            self.logger.set_level(new_level.upper())
            
            self.logger.info(
                f"Log level changed: {old_level} → {new_level.upper()}",
                request_id=request_id,
                issued_by=message.metadata.issued_by
            )
            
            # Send success response
            await self._send_control_finished_response(
                message,
                response_data={
                    "log_level_updated": True,
                    "previous_level": old_level,
                    "new_level": new_level.upper()
                }
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_handle_set_log_level",
                request_id=request_id
            )
            self.logger.error(
                "Failed to set log level",
                error_id=error.error_id,
                request_id=request_id
            )
            await self._send_control_error_response(message, str(e), error.error_id)
    async def _handle_drain(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle DRAIN command.
        
        Stops accepting new tasks but continues processing current task.
        Used before maintenance or scale-down.
        """
        request_id = message.metadata.request_identifier
        
        self._is_draining = True
        
        self.logger.warning(
            "Worker entering DRAIN mode - no new tasks will be leased",
            request_id=request_id,
            issued_by=message.metadata.issued_by
        )
        
        # Send success response
        await self._send_control_finished_response(
            message,
            response_data={
                "draining": True,
                "message": "Worker will not lease new tasks"
            }
        )
    async def _handle_resume(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle RESUME command.
        
        Resumes accepting new tasks after DRAIN.
        """
        request_id = message.metadata.request_identifier
        
        self._is_draining = False
        
        self.logger.info(
            "Worker exiting DRAIN mode - will resume leasing tasks",
            request_id=request_id,
            issued_by=message.metadata.issued_by
        )
        
        # Send success response
        await self._send_control_finished_response(
            message,
            response_data={
                "draining": False,
                "message": "Worker will resume leasing tasks"
            }
        )
    async def _handle_shutdown(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle SHUTDOWN command.
        
        Initiates graceful shutdown:
        1. Stops leasing new tasks
        2. Completes current task (if any)
        3. Exits run loop
        """
        request_id = message.metadata.request_identifier
        
        grace_period = message.parameters.get("grace_period_seconds", 300)  # 5 min default
        
        self.logger.warning(
            "Worker SHUTDOWN requested",
            request_id=request_id,
            grace_period_seconds=grace_period,
            issued_by=message.metadata.issued_by
        )
        
        # Set shutdown flag (checked in run loop)
        self._shutdown_requested = True
        self._is_draining = True  # Also stop leasing new tasks
        
        # Send response
        await self._send_control_finished_response(
            message,
            response_data={
                "shutdown_initiated": True,
                "grace_period_seconds": grace_period,
                "message": "Worker will complete current task and exit"
            }
        )
        
        # Note: Actual shutdown happens in worker.run() loop
        # which checks self._shutdown_requested
    async def _handle_heartbeat_force(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle HEARTBEAT_FORCE command.
        
        Forces immediate heartbeat outside normal schedule.
        Used by backend to quickly check if worker is alive.
        """
        request_id = message.metadata.request_identifier
        
        try:
            # Get current health
            health_status = await self.health_monitor.get_overall_status()
            health_checks = await self.health_monitor.get_all_check_results()
            
            # Send heartbeat immediately
            heartbeat_message = ComponentMetricsHealthMessage(
                metadata=MetricsHealthMetadata(
                    event_type=RegistrationEventType.COMPONENT_HEARTBEAT,
                    component_id=self.worker_id,
                    component_type=self.component_type,
                    nodegroup=self.nodegroup
                ),
                health_status=health_status,
                health_checks=health_checks
            )
            
            await self.kafka_metrics_producer.publish_message(heartbeat_message)
            
            self.logger.info(
                "Forced heartbeat sent",
                request_id=request_id,
                health_status=health_status.value
            )
            
            # Send success response
            await self._send_control_finished_response(
                message,
                response_data={
                    "heartbeat_sent": True,
                    "health_status": health_status.value,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_handle_heartbeat_force",
                request_id=request_id
            )
            await self._send_control_error_response(message, str(e), error.error_id)


    async def _send_control_working_response(
        self,
        message: WorkerControlMessage
    ) -> None:
        """Send immediate 'Working' response to acknowledge command receipt."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=message.metadata.request_identifier,
                response_type="status",
                status="Working",
                component_id=self.worker_id,
                nodegroup=self.nodegroup
            ),
            response_data=None
        )
        
        await self.kafka_response_producer.publish_response(response)

    async def _send_control_finished_response(
        self,
        message: WorkerControlMessage,
        response_data: Optional[Dict[str, Any]] = None
    ) -> None:
        """Send 'Finished' response after successful command execution."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=message.metadata.request_identifier,
                response_type="inline",
                status="Finished",
                component_id=self.worker_id,
                nodegroup=self.nodegroup
            ),
            response_data=response_data or {}
        )
        
        await self.kafka_response_producer.publish_response(response)

    async def _send_control_error_response(
        self,
        message: WorkerControlMessage,
        error_message: str,
        error_id: Optional[str] = None
    ) -> None:
        """Send 'FinishedWithError' response after failed command execution."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=message.metadata.request_identifier,
                response_type="status",
                status="FinishedWithError",
                component_id=self.worker_id,
                nodegroup=self.nodegroup
            ),
            error_message=error_message,
            response_data={"error_id": error_id} if error_id else None
        )
        
        await self.kafka_response_producer.publish_response(response)


    async def _main_loop_async_to_delete(self):
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
                    #print(f"WORKER {self.worker_id}: No work available, about to sleep")
                    await asyncio.sleep(10)
                    
            except Exception as e:
                print(f"WORKER {self.worker_id}: In Exception")
                error = self.error_handler.handle_error(
                    e, "worker_main_loop",
                    operation="main_loop_iteration",
                    worker_id=self.worker_id
                )
                self.logger.error("Error in worker main loop", error_id=error.error_id)
                
                # Back off on repeated failures
                await asyncio.sleep(5)

    async def _get_task_from_orchestrator_async(self) -> Optional[WorkPacket]:
        """
        Get the next available task by calling the configured backend interface.
        This method abstracts away the deployment mode (single-process vs. EKS).
        """
        try:
            # Log attempt to get task
            self.logger.debug(f"Worker {self.worker_id} requesting next task via backend.", worker_id=self.worker_id)

            # Call the get_next_task method on the initialized backend instance.
            # The backend instance (DirectDBBackend or APIBackend) handles the specifics
            # of how to get the task (queue access or API call).
            work_packet: Optional[WorkPacket] = await self.backend.get_next_task(
                worker_id=self.worker_id,
                node_group=self.settings.system.node_group # Pass the worker's node group
            )

            if work_packet:
                self.logger.info(
                    f"Worker {self.worker_id} received task {work_packet.header.task_id} via backend.",
                    worker_id=self.worker_id,
                    task_id=work_packet.header.task_id,
                    task_type=work_packet.payload.task_type.value # Access enum value
                )
                # Ensure the returned object is indeed a WorkPacket (Pydantic validation happens in backend)
                if not isinstance(work_packet, WorkPacket):
                     self.logger.error("Backend returned invalid WorkPacket type.", received_type=type(work_packet))
                     return None
                return work_packet
            else:
                # No task was available or assigned by the backend
                self.logger.debug(f"Worker {self.worker_id}: No task received from backend.", worker_id=self.worker_id)
                return None

        except Exception as e:
            # Handle potential errors during the backend call (network, DB, etc.)
            error = self.error_handler.handle_error(
                e, "get_task_from_orchestrator_async", # Updated context identifier
                operation="backend_communication",
                worker_id=self.worker_id
            )
            # Log the error but return None, allowing the worker loop to retry later
            self.logger.warning(
                f"Worker {self.worker_id} failed to get task from backend.",
                error_id=error.error_id,
                error_message=str(e),
                worker_id=self.worker_id
            )
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
            if task.task_type == "DISCOVERY_ENUMERATE":
                await self._process_discovery_enumerate_async(work_packet)
            
            elif task.task_type == "DISCOVERY_GET_DETAILS":
                await self._process_discovery_get_details_async(work_packet)
            
            elif task.task_type == "CLASSIFICATION":
                await self._process_classification_async(work_packet)
            
            elif task.task_type == "POLICY_ACTION_EXECUTE":
                await self._process_policy_action_execute_async(work_packet)
            elif work_packet.payload.task_type == TaskType.DATASOURCE_PROFILE:
                await self._process_datasource_profile_async(work_packet)        
            elif work_packet.payload.task_type == TaskType.BENCHMARK_EXECUTE:
                await self._process_benchmark_async(work_packet)        
            elif work_packet.payload.task_type == TaskType.ENTITLEMENT_EXTRACT:
                await self._process_entitlement_async(work_packet)        


            # PolicyWorker task types should NOT reach here

            elif task.task_type in ("POLICY_QUERY_EXECUTE", "PREPARE_CLASSIFICATION_TASKS", "JOB_CLOSURE"):
                raise ProcessingError(
                    message=f"Worker received PolicyWorker task type: {task.task_type}",
                    error_type=ErrorType.CONFIGURATION_INVALID,
                    severity=ErrorSeverity.HIGH,
                    context={"task_id": task.id.hex(), "task_type": task.task_type}
                )
            
            else:
                raise ProcessingError(
                    message=f"Unknown task type: {task.task_type}",
                    error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    context={"task_id": task.id.hex()}
                )


            
                
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


    async def _complete_task_with_kafka_upload(
        self,
        task_id: str,
        job_id: int,
        result_payload: Dict[str, Any],
        context: Dict[str, Any]
    ):
        """
        Complete task with Kafka upload (upload-during-completion pattern).
        
        NEW METHOD for G2
        
        Logic:
        1. If Kafka enabled and local file exists: Upload to Kafka
        2. If upload succeeds: Delete local file, mark task COMPLETED
        3. If upload fails: Keep local file, mark task FAILED
        4. If Kafka disabled: Mark task COMPLETED (data already in DB)
        
        CRITICAL: Task is only marked COMPLETED after successful upload.
        """
        upload_successful = False
        
        try:
            # Check if we have a local file to upload
            if (self.current_local_file_writer and 
                self.kafka_producer and 
                self.kafka_producer.is_available()):
                
                # Get task parameters for topic routing
                task_type = self.current_task.payload.task_type if self.current_task else "unknown"
                datasource_id = result_payload.get("datasource_id", "unknown")
                
                # Determine topic type based on task type
                if task_type == "DISCOVERY":
                    topic_type = "discovery"
                elif task_type == "CLASSIFICATION":
                    topic_type = "findings"
                else:
                    topic_type = "findings"  # Default
                
                self.logger.info(
                    f"Starting Kafka upload for completed task",
                    task_id=task_id,
                    topic_type=topic_type,
                    records_written=self.current_local_file_writer.get_records_written(),
                    **context
                )
                
                # Upload to Kafka
                upload_successful = await self.kafka_producer.upload_file_to_kafka(
                    file_writer=self.current_local_file_writer,
                    topic_type=topic_type,
                    datasource_id=datasource_id,
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self.worker_id,
                    trace_id=context.get("trace_id"),
                    context=context
                )
                
                if upload_successful:
                    # Delete local file after successful upload
                    await self.current_local_file_writer.cleanup()
                    
                    # Update result payload with upload info
                    result_payload["kafka_upload"] = {
                        "status": "success",
                        "records_sent": self.current_local_file_writer.get_records_written(),
                        "topic_type": topic_type
                    }
                    
                    # Report task completion
                    await self.backend.report_task_completion(
                        task_id=task_id,
                        status="COMPLETED",
                        result_payload=result_payload,
                        context=context
                    )
                    
                    self.logger.info(
                        f"Task completed with successful Kafka upload",
                        records_sent=self.current_local_file_writer.get_records_written(),
                        **context
                    )
                    
                else:
                    # Upload failed - keep local file for recovery
                    result_payload["kafka_upload"] = {
                        "status": "failed",
                        "local_file_path": str(self.current_local_file_writer.get_file_path()),
                        "records_pending": self.current_local_file_writer.get_records_written()
                    }
                    
                    # Report task as FAILED
                    await self.backend.report_task_completion(
                        task_id=task_id,
                        status="FAILED",
                        result_payload=result_payload,
                        context=context
                    )
                    
                    self.logger.error(
                        f"Task marked FAILED - Kafka upload failed, local file preserved",
                        local_file=str(self.current_local_file_writer.get_file_path()),
                        **context
                    )
                    
            else:
                # No local file (Kafka disabled or direct DB writes)
                # Task already completed via database writes
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="COMPLETED",
                    result_payload=result_payload,
                    context=context
                )
                
                self.logger.info(
                    f"Task completed (database mode)",
                    **context
                )
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "worker_complete_task_kafka",
                task_id=task_id,
                **context
            )
            
            # Report failure
            result_payload["error"] = {
                "error_id": error.error_id,
                "error_type": str(error.error_type),
                "error_message": str(e)
            }
            
            await self.backend.report_task_completion(
                task_id=task_id,
                status="FAILED",
                result_payload=result_payload,
                context=context
            )
            
            raise


    async def _handle_datasource_config_update(self, notification: Dict[str, Any]):
        """
        Handle datasource configuration update notification from Kafka.
        
        NEW METHOD for G2 (Kafka consumer callback)
        
        Called when control topic receives datasource config update.
        """
        try:
            datasource_id = notification.get("api_parameters", {}).get("datasource_id")
            
            if not datasource_id:
                self.logger.warning(
                    "Datasource config update missing datasource_id",
                    notification=notification
                )
                return
            
            # Fetch updated config from backend
            new_config = await self.backend.get_datasource_configuration(
                datasource_id,
                {"worker_id": self.worker_id, "source": "kafka_control_topic"}
            )
            
            self.logger.info(
                "Datasource configuration updated",
                datasource_id=datasource_id,
                worker_id=self.worker_id
            )
            
            # TODO: Apply configuration to running tasks if needed
            # For now, new tasks will pick up the updated config
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "worker_handle_datasource_update",
                datasource_id=datasource_id
            )
            self.logger.error(
                "Failed to handle datasource config update",
                error_id=error.error_id
            )



    async def _handle_connector_config_update(self, notification: Dict[str, Any]):
        """
        Handle connector configuration update notification from Kafka.
        
        NEW METHOD for G2 (Kafka consumer callback)
        """
        try:
            connector_type = notification.get("api_parameters", {}).get("connector_type")
            
            if not connector_type:
                self.logger.warning(
                    "Connector config update missing connector_type",
                    notification=notification
                )
                return
            
            # Fetch updated config from backend
            new_config = await self.backend.get_connector_configuration(
                connector_type,
                {"worker_id": self.worker_id, "source": "kafka_control_topic"}
            )
            
            self.logger.info(
                "Connector configuration updated",
                connector_type=connector_type,
                worker_id=self.worker_id
            )
            
            # TODO: Apply configuration to connector factory
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "worker_handle_connector_update",
                connector_type=connector_type
            )
            self.logger.error(
                "Failed to handle connector config update",
                error_id=error.error_id
            )


    async def _process_benchmark_async(
        self,
        work_packet: WorkPacket
    ) -> None:
        """
        Processes BENCHMARK_EXECUTE task using local-write-first architecture.
        
        NEW METHOD - does not exist in current worker.py.
        
        Flow:
            1. Get system profile from connector
            2. Call backend API to resolve QuerySet for benchmark
            3. Create LocalFileWriter for benchmark findings
            4. Execute each query via connector
            5. Write BenchmarkFindings to writer
            6. Call _finalize_task_output to upload/write
            
        Requirements:
            - Backend must implement get_queryset_for_benchmark() method
            - Connector must implement execute_benchmark_query() method
        """
        payload = work_packet.payload
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        datasource_id = payload.datasource_id
        benchmark_name = payload.benchmark_name  # e.g., "CIS_SQL_Server_v1.2"
        
        task_context = {
            "job_id": job_id,
            "task_id": task_id,
            "datasource_id": datasource_id,
            "benchmark_name": benchmark_name,
            "trace_id": work_packet.header.trace_id,
            "task_type": TaskType.BENCHMARK_EXECUTE
        }
        
        processing_errors = []
        
        try:
            # Get datasource configuration
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id, task_context
            )
            
            # Get connector
            connector = await self._get_connector(datasource_config, task_context)
            
            # Get system profile (for query resolution)
            profile = await connector.get_system_profile()
            
            # Call backend to resolve QuerySet based on profile and benchmark
            queries = await self.backend.get_queryset_for_benchmark(
                benchmark_name=benchmark_name,
                system_profile=profile,
                context=task_context
            )
            
            # Create LocalFileWriter for benchmark findings
            benchmark_writer = LocalFileWriter(
                task_id=f"{task_id}_benchmark",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            # Open writer and execute benchmark
            async with benchmark_writer:
                # Execute each query
                for query in queries:
                    try:
                        # Execute query via connector
                        results = await connector.execute_benchmark_query(query)
                        
                        # results is list of BenchmarkFinding models
                        for finding in results:
                            await benchmark_writer.append_record(finding.model_dump())
                            
                    except Exception as e:
                        error = self.error_handler.handle_error(
                            e,
                            context="execute_benchmark_query",
                            query_id=query.get("query_id", "unknown"),
                            **task_context
                        )
                        processing_errors.append({
                            "query_id": query.get("query_id", "unknown"),
                            "error_id": error.error_id,
                            "error": str(e),
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
            
            # Writer is now closed and flushed
            
            # Finalize output
            success = await self._finalize_task_output(
                file_writers={
                    "benchmark": benchmark_writer
                },
                topic_type_map={
                    "benchmark": "benchmark_findings"
                },
                event_type_map={
                    "benchmark": WorkerResultEventType.BENCHMARK_FINDINGS
                },
                task_context=task_context,
                processing_errors=processing_errors
            )
            
            if not success:
                self.logger.error(
                    "Task finalization failed",
                    **task_context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_process_benchmark_async",
                **task_context
            )
            self.logger.error(
                "Critical failure in benchmark execution",
                error_id=error.error_id,
                **task_context
            )
            
            # Send failure status
            status_payload = TaskStatusUpdatePayload(
                status="FAILED",
                processing_errors=[{
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }],
                upload_errors=[]
            )
            
            try:
                await self._send_task_status_update(status_payload, task_context)
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="FAILED",
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except:
                pass



    async def _process_entitlement_async(
        self,
        work_packet: WorkPacket
    ) -> None:
        """
        Processes ENTITLEMENT_EXTRACT task using local-write-first architecture.
        
        NEW METHOD - does not exist in current worker.py.
        
        Flow:
            1. Create LocalFileWriter for entitlement snapshots
            2. Get connector and fetch entitlement data
            3. Write EntitlementSnapshot to writer
            4. Call _finalize_task_output to upload/write
            
        Requirements:
            - Connector must implement get_entitlement_data() method
            - Returns EntitlementSnapshot model with user/group permissions
        """
        payload = work_packet.payload
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        datasource_id = payload.datasource_id
        
        task_context = {
            "job_id": job_id,
            "task_id": task_id,
            "datasource_id": datasource_id,
            "trace_id": work_packet.header.trace_id,
            "task_type": TaskType.ENTITLEMENT_EXTRACT
        }
        
        processing_errors = []
        
        try:
            # Get datasource configuration
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id, task_context
            )
            
            # Get connector
            connector = await self._get_connector(datasource_config, task_context)
            
            # Verify connector supports entitlement extraction
            if not hasattr(connector, 'get_entitlement_data'):
                raise ProcessingError(
                    f"Connector does not support entitlement extraction",
                    ErrorType.PROCESSING_CRITICAL_FAILURE,
                    datasource_type=datasource_config.connection.connection_type
                )
            
            # Create LocalFileWriter for entitlements
            entitlement_writer = LocalFileWriter(
                task_id=f"{task_id}_entitlement",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            # Open writer and extract entitlements
            async with entitlement_writer:
                # Get entitlement snapshot from connector
                snapshot = await connector.get_entitlement_data(work_packet)
                
                # Write snapshot
                await entitlement_writer.append_record(snapshot.model_dump())
            
            # Writer is now closed and flushed
            
            # Finalize output
            success = await self._finalize_task_output(
                file_writers={
                    "entitlement": entitlement_writer
                },
                topic_type_map={
                    "entitlement": "entitlement_snapshots"
                },
                event_type_map={
                    "entitlement": WorkerResultEventType.ENTITLEMENT_SNAPSHOTS
                },
                task_context=task_context,
                processing_errors=processing_errors
            )
            
            if not success:
                self.logger.error(
                    "Task finalization failed",
                    **task_context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_process_entitlement_async",
                **task_context
            )
            self.logger.error(
                "Critical failure in entitlement extraction",
                error_id=error.error_id,
                **task_context
            )
            
            # Send failure status
            status_payload = TaskStatusUpdatePayload(
                status="FAILED",
                processing_errors=[{
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }],
                upload_errors=[]
            )
            
            try:
                await self._send_task_status_update(status_payload, task_context)
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="FAILED",
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except:
                pass



    async def _process_datasource_profile_async(
        self,
        work_packet: WorkPacket
    ) -> None:
        """
        Processes DB_PROFILE task using new local-write-first architecture.
        
        COMPLETELY REWRITTEN for new architecture.
        
        Flow:
            1. Create LocalFileWriter for database profiles
            2. Get connector and fetch system profile
            3. Write SystemProfile to writer
            4. Call _finalize_task_output to upload/write
            
        Old Logic REMOVED:
            - db_interface.update_datasource_metadata (direct DB writes)
        """
        payload = work_packet.payload
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        datasource_id = payload.datasource_id
        
        task_context = {
            "job_id": job_id,
            "task_id": task_id,
            "datasource_id": datasource_id,
            "trace_id": work_packet.header.trace_id,
            "task_type": TaskType.DB_PROFILE
        }
        
        processing_errors = []
        
        try:
            # Get datasource configuration
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id, task_context
            )
            
            # Get connector
            connector = await self._get_connector(datasource_config, task_context)
            
            # Create LocalFileWriter for profiles
            profile_writer = LocalFileWriter(
                task_id=f"{task_id}_profile",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            # Open writer and get profile
            async with profile_writer:
                # Get system profile from connector
                profile = await connector.get_system_profile()
                
                # Write profile
                await profile_writer.append_record(profile.model_dump())
            
            # Writer is now closed and flushed
            
            # Finalize output
            success = await self._finalize_task_output(
                file_writers={
                    "profile": profile_writer
                },
                topic_type_map={
                    "profile": "database_profiles"
                },
                event_type_map={
                    "profile": WorkerResultEventType.DATABASE_PROFILES
                },
                task_context=task_context,
                processing_errors=processing_errors
            )
            
            if not success:
                self.logger.error(
                    "Task finalization failed",
                    **task_context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_process_datasource_profile_async",
                **task_context
            )
            self.logger.error(
                "Critical failure in datasource profiling",
                error_id=error.error_id,
                **task_context
            )
            
            # Send failure status
            status_payload = TaskStatusUpdatePayload(
                status="FAILED",
                processing_errors=[{
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }],
                upload_errors=[]
            )
            
            try:
                await self._send_task_status_update(status_payload, task_context)
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="FAILED",
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except:
                pass



    async def _process_discovery_enumerate_async(
        self,
        work_packet: WorkPacket
    ) -> None:
        """
        Processes DISCOVERY_ENUMERATE task using new local-write-first architecture.
        
        COMPLETELY REWRITTEN for new architecture.
        
        Flow:
            1. Create two LocalFileWriters (discovery + tor)
            2. Get connector and enumerate objects
            3. Write DiscoveredObjects to discovery writer
            4. Write TaskOutputRecords (boundaries) to tor writer
            5. Call _finalize_task_output to upload/write
            
        Old Logic REMOVED:
            - _insert_batch_to_staging_async (direct DB writes)
            - _report_task_progress_async (direct DB writes)
        """
        payload = work_packet.payload
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        datasource_id = payload.datasource_id
        
        task_context = {
            "job_id": job_id,
            "task_id": task_id,
            "datasource_id": datasource_id,
            "trace_id": work_packet.header.trace_id,
            "task_type": TaskType.DISCOVERY_ENUMERATE
        }
        
        processing_errors = []
        
        try:
            # Get datasource configuration
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id, task_context
            )
            
            # Get connector
            connector = await self._get_connector(datasource_config, task_context)
            
            # Create LocalFileWriters for each output type
            discovery_writer = LocalFileWriter(
                task_id=f"{task_id}_discovery",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            tor_writer = LocalFileWriter(
                task_id=f"{task_id}_tor",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            # Open writers
            async with discovery_writer:
                async with tor_writer:
                    # Enumerate objects via connector
                    async for batch in connector.enumerate_objects(work_packet):
                        # batch is DiscoveryBatch with discovered_objects list
                        
                        # Separate files vs directories
                        files_to_classify = []
                        new_boundaries = []
                        
                        for obj in batch.discovered_objects:
                            if obj.object_type == ObjectType.FILE:
                                files_to_classify.append(obj)
                            elif obj.object_type == ObjectType.DIRECTORY:
                                new_boundaries.append(obj)
                        
                        # Write discovered objects (files/tables) to discovery writer
                        if files_to_classify:
                            for obj in files_to_classify:
                                try:
                                    await discovery_writer.append_record(obj.model_dump())
                                except Exception as e:
                                    error = self.error_handler.handle_error(
                                        e,
                                        context="write_discovered_object",
                                        object_path=obj.object_path,
                                        **task_context
                                    )
                                    processing_errors.append({
                                        "object_path": obj.object_path,
                                        "error_id": error.error_id,
                                        "error": str(e),
                                        "timestamp": datetime.now(timezone.utc).isoformat()
                                    })
                        
                        # Write boundaries as TaskOutputRecords for Pipeliner
                        if new_boundaries:
                            tor = TaskOutputRecord(
                                output_type="NEW_ENUMERATION_BOUNDARIES",
                                output_payload={
                                    "discovered_objects": [obj.model_dump() for obj in new_boundaries]
                                }
                            )
                            try:
                                await tor_writer.append_record(tor.model_dump())
                            except Exception as e:
                                error = self.error_handler.handle_error(
                                    e,
                                    context="write_task_output_record",
                                    **task_context
                                )
                                processing_errors.append({
                                    "error_id": error.error_id,
                                    "error": str(e),
                                    "timestamp": datetime.now(timezone.utc).isoformat()
                                })
            
            # Writers are now closed and flushed
            
            # Finalize output (upload to Kafka or write to DB)
            success = await self._finalize_task_output(
                file_writers={
                    "discovery": discovery_writer,
                    "tor": tor_writer
                },
                topic_type_map={
                    "discovery": "discovery",
                    "tor": "task_outputs"
                },
                event_type_map={
                    "discovery": WorkerResultEventType.DISCOVERED_OBJECTS,
                    "tor": WorkerResultEventType.TASK_OUTPUT_RECORDS
                },
                task_context=task_context,
                processing_errors=processing_errors
            )
            
            if not success:
                self.logger.error(
                    "Task finalization failed",
                    **task_context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_process_discovery_enumerate_async",
                **task_context
            )
            self.logger.error(
                "Critical failure in discovery enumeration",
                error_id=error.error_id,
                **task_context
            )
            
            # Send failure status
            status_payload = TaskStatusUpdatePayload(
                status="FAILED",
                processing_errors=[{
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }],
                upload_errors=[]
            )
            
            try:
                await self._send_task_status_update(status_payload, task_context)
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="FAILED",
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except:
                pass  # Best effort


    async def _process_discovery_get_details_async(
        self,
        work_packet: WorkPacket
    ) -> None:
        """
        Processes DISCOVERY_GET_DETAILS task using new local-write-first architecture.
        
        COMPLETELY REWRITTEN for new architecture.
        
        Flow:
            1. Create LocalFileWriter for object_metadata
            2. Get connector and fetch object details
            3. Write ObjectMetadata records to writer
            4. Call _finalize_task_output to upload/write
            
        Old Logic REMOVED:
            - db_interface.upsert_object_metadata (direct DB writes)
        """
        payload = work_packet.payload
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        datasource_id = payload.datasource_id
        
        task_context = {
            "job_id": job_id,
            "task_id": task_id,
            "datasource_id": datasource_id,
            "trace_id": work_packet.header.trace_id,
            "task_type": TaskType.DISCOVERY_GET_DETAILS
        }
        
        processing_errors = []
        
        try:
            # Get datasource configuration
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id, task_context
            )
            
            # Get connector
            connector = await self._get_connector(datasource_config, task_context)
            
            # Create LocalFileWriter for object metadata
            metadata_writer = LocalFileWriter(
                task_id=f"{task_id}_metadata",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            # Open writer and fetch details
            async with metadata_writer:
                # Get object details from connector
                metadata_records = await connector.get_object_details(work_packet)
                
                # Write each metadata record
                for metadata in metadata_records:
                    try:
                        await metadata_writer.append_record(metadata.model_dump())
                    except Exception as e:
                        error = self.error_handler.handle_error(
                            e,
                            context="write_object_metadata",
                            object_hash=metadata.object_key_hash.hex() if isinstance(metadata.object_key_hash, bytes) else str(metadata.object_key_hash),
                            **task_context
                        )
                        processing_errors.append({
                            "object_hash": metadata.object_key_hash.hex() if isinstance(metadata.object_key_hash, bytes) else str(metadata.object_key_hash),
                            "error_id": error.error_id,
                            "error": str(e),
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
            
            # Writer is now closed and flushed
            
            # Finalize output
            success = await self._finalize_task_output(
                file_writers={
                    "metadata": metadata_writer
                },
                topic_type_map={
                    "metadata": "object_metadata"
                },
                event_type_map={
                    "metadata": WorkerResultEventType.OBJECT_METADATA
                },
                task_context=task_context,
                processing_errors=processing_errors
            )
            
            if not success:
                self.logger.error(
                    "Task finalization failed",
                    **task_context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_process_discovery_get_details_async",
                **task_context
            )
            self.logger.error(
                "Critical failure in discovery get details",
                error_id=error.error_id,
                **task_context
            )
            
            # Send failure status
            status_payload = TaskStatusUpdatePayload(
                status="FAILED",
                processing_errors=[{
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }],
                upload_errors=[]
            )
            
            try:
                await self._send_task_status_update(status_payload, task_context)
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="FAILED",
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except:
                pass




    async def _process_classification_async(
        self,
        work_packet: WorkPacket
    ) -> None:
        """
        Processes CLASSIFICATION task using new local-write-first architecture.
        
        COMPLETELY REWRITTEN for new architecture.
        
        Flow:
            1. Create LocalFileWriter for classification findings
            2. Initialize classifier engine
            3. Get connector and iterate content components
            4. Write findings to writer
            5. Call _finalize_task_output to upload/write
            
        Old Logic REMOVED:
            - _store_classification_results_async (direct DB writes)
            - db_interface.insert_scan_findings (direct DB writes)
        To Do:
            - Missing _processing error handling in _process_file_based_classification_async and _process_database_classification_async (Specifically Access error happening as required for calculation of classification date 
            
        """
        payload = work_packet.payload
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        datasource_id = payload.datasource_id
        
        task_context = {
            "job_id": job_id,
            "task_id": task_id,
            "datasource_id": datasource_id,
            "trace_id": work_packet.header.trace_id,
            "task_type": TaskType.CLASSIFICATION
        }
        
        processing_errors = []
        
        try:
            # Get datasource configuration
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id, task_context
            )
            
            # Get classifier template
            classifier_template = await self.backend.get_classifier_template(
                payload.classifier_template_id, task_context
            )
            
            # Initialize classifier engine
            engine_interface = await self._initialize_engine(
                classifier_template, task_context
            )
            
            # Get connector
            connector = await self._get_connector(datasource_config, task_context)
            
            # Create LocalFileWriter for findings
            findings_writer = LocalFileWriter(
                task_id=f"{task_id}_findings",
                fallback_directory=self.settings.worker.local_buffer_dir,
                max_file_size_mb=self.settings.worker.max_file_size_mb,
                flush_interval_records=self.settings.worker.flush_interval_records,
                logger=self.logger,
                error_handler=self.error_handler,
                context=task_context
            )
            
            # Open writer and classify
            async with findings_writer:
                # Iterate through content components
                db_records=[]
                if isinstance(connector, IFileDataSourceConnector):
                    db_records=await self._process_file_based_classification_async(connector, engine_interface, work_packet)
                    for finding_record in db_records:
                        await findings_writer.append_record(finding_record)
                    
                elif isinstance(connector, IDatabaseDataSourceConnector):
                    db_records=await self._process_database_classification_async(connector, engine_interface, work_packet)
                    for finding_record in db_records:
                        await findings_writer.append_record(finding_record)

                else:
                    raise TypeError(f"Connector for datasource {payload.datasource_id} has an unknown type.")
                            
            
            
            # Finalize output
            success = await self._finalize_task_output(
                file_writers={
                    "findings": findings_writer
                },
                topic_type_map={
                    "findings": "findings"
                },
                event_type_map={
                    "findings": WorkerResultEventType.CLASSIFICATION_FINDINGS
                },
                task_context=task_context,
                processing_errors=null
            )
            
            if not success:
                self.logger.error(
                    "Task finalization failed",
                    **task_context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_process_classification_async",
                **task_context
            )
            self.logger.error(
                "Critical failure in classification",
                error_id=error.error_id,
                **task_context
            )
            
            # Send failure status
            status_payload = TaskStatusUpdatePayload(
                status="FAILED",
                processing_errors=[{
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }],
                upload_errors=[]
            )
            
            try:
                await self._send_task_status_update(status_payload, task_context)
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status="FAILED",
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except:
                pass




    async def _process_file_based_classification_async(self, connector: IFileDataSourceConnector, 
                                                     engine_interface: EngineInterface, work_packet: WorkPacket)-> List[Dict[str, Any]]:
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
                self.logger.warning(
                    f"Component classification failed for component '{content_component.component_id}'",
                    exc_info=True,  # This adds the full traceback
                    component_id=content_component.component_id,
                    parent_path=content_component.parent_path,
                    job_id=work_packet.header.job_id,
                    task_id=work_packet.header.task_id
                )
                # Continue processing other components
            
            # Send heartbeat periodically
            if components_processed % 10 == 0:
                await self._send_heartbeat_async(work_packet.header.task_id)

        # Convert ALL accumulated findings to database format
        self.logger.info("Converting findings to database format",
                        task_id=work_packet.header.task_id,
                        total_findings=len(all_findings),
                        total_components=total_components)

        for finding in all_findings:
            
            print(f"Worker:For file: {finding.context_data.get('file_path')},  Finding : {finding.classifier_id}, text='{finding.text}'")
        db_records =  engine_interface.convert_findings_to_db_format(
            all_findings=all_findings,
            total_rows_scanned=total_components  # For files, this represents component count
        )
        print(f"[WORKER] convert_findings_to_db_format created {len(db_records)} records")
        for rec in db_records:
            print(f"Worker:  Record: classifier={rec['classifier_id']}, finding_count={rec['finding_count']},, file_path={rec['file_path']}")        
        # Store results in database (ASYNC)
        return db_records
        #await self._store_classification_results_async(db_records, work_packet)



    async def _process_database_classification_async(self, connector: IDatabaseDataSourceConnector,
                                                 engine_interface: EngineInterface, work_packet: WorkPacket)-> List[Dict[str, Any]]:
        """Correctly handles database connectors using strategy-based table processing."""
        all_findings = []
        total_rows_scanned = 0
        
        self.logger.info("Starting database classification (strategy-based)", 
                        task_id=work_packet.header.task_id)

        # The connector yields batches of rows for each table/column it processes
        async for content_batch in connector.get_object_content(work_packet):
            try:
                # Extract metadata and row data from the batch
                metadata = content_batch.get("metadata", {})
                rows = content_batch.get("content", [])
            
                if not isinstance(rows, list):
                    self.logger.warning(
                        "Database content for classification was not a list of rows.", 
                        object_id=metadata.get("object_id")
                    )
                    continue
                
                total_rows_scanned += len(rows)
                
                self.logger.info(
                    f"Processing table with {len(rows)} rows using strategy-based processing",
                    row_count=len(rows),
                    object_path=metadata.get('object_path')
                )
                
                # NEW: Single call to strategy-based table processing
                table_findings = await engine_interface.classify_database_table(
                    table_data=rows,
                    table_metadata=metadata
                )
                
                all_findings.extend(table_findings)
                
            except Exception as e:
                self.error_handler.handle_error(
                    e, 
                    "classify_database_batch", 
                    object_path=metadata.get("object_path")
                )
                continue  # Skip problematic batches

        self.logger.info(
            "Database classification completed",
            task_id=work_packet.header.task_id,
            total_findings=len(all_findings),
            total_rows_scanned=total_rows_scanned
        )

        # Convert findings to database format
        db_records = engine_interface.convert_findings_to_db_format(
            all_findings=all_findings,
            total_rows_scanned=total_rows_scanned
        )
        
        #await self._store_classification_results_async(db_records, work_packet)
        return db_records


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
            self.logger.error("Hello Component classification failed",exc_info=True,
                             component_id=component.component_id,
                             error=str(e))
            return []


    async def _classify_table_component_async(self, component: ContentComponent, 
                                            engine_interface: EngineInterface, 
                                            work_packet: WorkPacket) -> List[PIIFinding]:
        """
        Handle table component with quality assessment and strategy-based processing.
        
        Routes to either:
        - Strategy-based table processing (if quality passes)
        - Text-based processing (if quality fails)
        """
        try:
            # Single call to new method that handles quality assessment and routing
            findings = await engine_interface.classify_table_component(component)
            
            self.logger.debug(
                "Table component classification completed",
                component_id=component.component_id,
                findings_count=len(findings)
            )
            
            return findings
            
        except Exception as e:
            self.logger.error(
                "Table component classification failed",
                exc_info=True,
                component_id=component.component_id,
                parent_path=component.parent_path,
                job_id=work_packet.header.job_id,
                task_id=work_packet.header.task_id
            )
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
            await self.db_interface.insert_scan_findings(db_records, context=job_context)
            
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


    async def _finalize_task_output(
        self,
        file_writers: Dict[str, LocalFileWriter],
        topic_type_map: Dict[str, str],
        event_type_map: Dict[str, WorkerResultEventType],
        task_context: Dict[str, Any],
        processing_errors: List[Dict[str, Any]]
    ) -> bool:
        """
        Central orchestration method for task finalization.
        
        Handles both Kafka (distributed) and Database (standalone) modes.
        ALWAYS sends TASK_STATUS_UPDATE to Kafka and reports to backend.
        
        Args:
            file_writers: Dict mapping writer_name → LocalFileWriter instance
                         e.g., {"discovery": disco_writer, "tor": tor_writer}
            topic_type_map: Dict mapping writer_name → topic_type string
                           e.g., {"discovery": "discovery", "tor": "task_outputs"}
            event_type_map: Dict mapping writer_name → WorkerResultEventType
                           e.g., {"discovery": DISCOVERED_OBJECTS, "tor": TASK_OUTPUT_RECORDS}
            task_context: Context dict with job_id, task_id, datasource_id, etc.
            processing_errors: List of errors collected during task processing
            
        Returns:
            True if finalization succeeded, False otherwise
            
        Logic:
            1. Check results_destination flag
            2. Upload/write each file
            3. Aggregate metrics from all writers
            4. Construct TaskStatusUpdatePayload
            5. Send TASK_STATUS_UPDATE (always)
            6. Call backend.report_task_completion() (always)
            7. Cleanup files on success
        """
        upload_errors = []
        all_success = True
        
        # Extract context
        job_id = task_context["job_id"]
        task_id = task_context["task_id"]
        datasource_id = task_context["datasource_id"]
        worker_id = self.worker_id
        trace_id = task_context.get("trace_id")
        
        try:
            # Step 1: Check destination mode
            results_destination = self.settings.worker.results_destination
            
            # Step 2: Upload or write each file
            for writer_name, file_writer in file_writers.items():
                try:
                    if results_destination == "kafka":
                        # Kafka mode: Upload via KafkaProducer
                        topic_type = topic_type_map[writer_name]
                        
                        success = await self.kafka_producer.upload_file_to_kafka(
                            file_writer=file_writer,
                            topic_type=topic_type,
                            datasource_id=datasource_id,
                            job_id=job_id,
                            task_id=task_id,
                            worker_id=worker_id,
                            trace_id=trace_id,
                            context=task_context
                        )
                        
                        if not success:
                            all_success = False
                            upload_errors.append({
                                "writer": writer_name,
                                "topic_type": topic_type,
                                "error": "Kafka upload failed",
                                "timestamp": datetime.now(timezone.utc).isoformat()
                            })
                            
                    else:  # "database"
                        # Database mode: Write directly to DB
                        event_type = event_type_map[writer_name]
                        
                        summary, errors = await self._write_local_file_to_db(
                            file_writer=file_writer,
                            event_type=event_type,
                            job_id=job_id,
                            context=task_context
                        )
                        
                        if errors:
                            all_success = False
                            upload_errors.extend(errors)
                            
                except Exception as e:
                    all_success = False
                    error = self.error_handler.handle_error(
                        e,
                        context="_finalize_task_output_upload",
                        writer_name=writer_name,
                        **task_context
                    )
                    upload_errors.append({
                        "writer": writer_name,
                        "error_id": error.error_id,
                        "error": str(e),
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
                    self.logger.error(
                        f"Failed to upload/write {writer_name}",
                        error_id=error.error_id,
                        **task_context
                    )
            
            # Step 3: Aggregate metrics from all writers
            total_records = sum(w.get_records_written() for w in file_writers.values())
            total_bytes = sum(w.get_bytes_written() for w in file_writers.values())
            
            task_summary = TaskSummaryMetrics(
                items_processed=total_records,
                bytes_processed=total_bytes
            )
            
            # Step 4: Construct TaskStatusUpdatePayload
            final_status = "COMPLETED" if all_success else "FAILED"
            
            status_payload = TaskStatusUpdatePayload(
                status=final_status,
                percent_done=100.0,
                task_summary=task_summary,
                processing_errors=processing_errors,
                upload_errors=upload_errors
            )
            
            # Step 5: Send TASK_STATUS_UPDATE to Kafka (ALWAYS, even in database mode)
            try:
                await self._send_task_status_update(
                    status_payload=status_payload,
                    task_context=task_context
                )
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    context="_finalize_task_output_status_update",
                    **task_context
                )
                self.logger.error(
                    "Failed to send TASK_STATUS_UPDATE",
                    error_id=error.error_id,
                    **task_context
                )
                all_success = False
            
            # Step 6: Report to backend (ALWAYS, for Orchestrator lease management)
            try:
                await self.backend.report_task_completion(
                    task_id=task_id,
                    status=final_status,
                    result_payload=status_payload.model_dump(),
                    context=task_context
                )
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    context="_finalize_task_output_backend_report",
                    **task_context
                )
                self.logger.warning(
                    "Failed to report task completion to backend",
                    error_id=error.error_id,
                    **task_context
                )
                # Don't mark as failed - this is non-critical
            
            # Step 7: Cleanup files on success
            if all_success:
                for writer_name, file_writer in file_writers.items():
                    try:
                        await file_writer.cleanup()
                    except Exception as e:
                        # Log but don't fail - cleanup is best-effort
                        self.logger.warning(
                            f"Failed to cleanup {writer_name} file",
                            writer_name=writer_name,
                            file_path=str(file_writer.get_file_path()),
                            error=str(e),
                            **task_context
                        )
            else:
                self.logger.warning(
                    "Local files preserved for recovery due to failures",
                    file_count=len(file_writers),
                    **task_context
                )
            
            return all_success
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_finalize_task_output",
                **task_context
            )
            self.logger.error(
                "Critical failure in task finalization",
                error_id=error.error_id,
                **task_context
            )
            return False

    async def _write_local_file_to_db(
        self,
        file_writer: LocalFileWriter,
        event_type: WorkerResultEventType,
        job_id: int,
        context: Dict[str, Any]
    ) -> Tuple[Dict[str, int], List[Dict[str, Any]]]:
        """
        Writes local file contents directly to database (standalone mode).
        
        Routes to appropriate bulk insert method based on event_type.
        
        Args:
            file_writer: LocalFileWriter with completed file
            event_type: Type of records in file
            job_id: Job identifier
            context: Logging context
            
        Returns:
            Tuple of (summary_metrics, errors)
            summary_metrics: {"records_written": N, "records_failed": M}
            errors: List of error dicts
            
        Logic:
            1. Read file in batches
            2. Route to correct bulk insert based on event_type
            3. Track successes and failures
            4. Return summary and errors
        """
        records_written = 0
        records_failed = 0
        errors = []
        
        try:
            # Read all records from file
            all_records = []
            async for batch in file_writer.read_for_upload(
                batch_size_bytes=1024 * 1024  # 1MB batches
            ):
                all_records.extend(batch)
            
            if not all_records:
                self.logger.info(
                    "No records to write to database",
                    event_type=event_type.value,
                    **context
                )
                return ({"records_written": 0, "records_failed": 0}, [])
            
            # Route to appropriate bulk insert method
            try:
                if event_type == WorkerResultEventType.DISCOVERED_OBJECTS:
                    await self.db_interface.bulk_insert_discovered_objects(
                        all_records, job_id, context
                    )
                    
                elif event_type == WorkerResultEventType.OBJECT_METADATA:
                    await self.db_interface.bulk_insert_object_metadata(
                        all_records, job_id, context
                    )
                    
                elif event_type == WorkerResultEventType.CLASSIFICATION_FINDINGS:
                    await self.db_interface.bulk_insert_classification_findings(
                        all_records, job_id, context
                    )
                    
                elif event_type == WorkerResultEventType.TASK_OUTPUT_RECORDS:
                    await self.db_interface.bulk_insert_task_output_records(
                        all_records, job_id, context
                    )
                    
                elif event_type == WorkerResultEventType.DATABASE_PROFILES:
                    await self.db_interface.bulk_insert_database_profiles(
                        all_records, job_id, context
                    )
                    
                elif event_type == WorkerResultEventType.ENTITLEMENT_SNAPSHOTS:
                    await self.db_interface.bulk_insert_entitlement_snapshots(
                        all_records, job_id, context
                    )
                    
                elif event_type == WorkerResultEventType.BENCHMARK_FINDINGS:
                    await self.db_interface.bulk_insert_benchmark_findings(
                        all_records, job_id, context
                    )
                    
                else:
                    raise ValueError(f"Unsupported event_type for database write: {event_type}")
                
                records_written = len(all_records)
                
                self.logger.info(
                    f"Successfully wrote {records_written} records to database",
                    event_type=event_type.value,
                    records_written=records_written,
                    **context
                )
                
            except Exception as e:
                records_failed = len(all_records)
                error = self.error_handler.handle_error(
                    e,
                    context="_write_local_file_to_db_bulk_insert",
                    event_type=event_type.value,
                    record_count=len(all_records),
                    **context
                )
                errors.append({
                    "event_type": event_type.value,
                    "record_count": len(all_records),
                    "error_id": error.error_id,
                    "error": str(e),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                self.logger.error(
                    "Failed to write records to database",
                    error_id=error.error_id,
                    event_type=event_type.value,
                    **context
                )
            
            return (
                {"records_written": records_written, "records_failed": records_failed},
                errors
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_write_local_file_to_db",
                event_type=event_type.value,
                **context
            )
            errors.append({
                "error_id": error.error_id,
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            return ({"records_written": 0, "records_failed": 0}, errors)

    async def _send_task_status_update(
        self,
        status_payload: TaskStatusUpdatePayload,
        task_context: Dict[str, Any]
    ) -> None:
        """
        Sends TASK_STATUS_UPDATE message to Kafka.
        
        This is ALWAYS sent, even in database mode, because:
        - Orchestrator needs status updates for monitoring
        - Flink needs final status for task tracking
        
        Args:
            status_payload: TaskStatusUpdatePayload with final status and metrics
            task_context: Context with task_id, job_id, datasource_id, etc.
            
        Raises:
            Exception: If send fails (caller should handle)
        """
        # Create message envelope
        message = {
            "metadata": {
                "message_id": str(uuid4()),
                "event_type": WorkerResultEventType.TASK_STATUS_UPDATE.value,
                "schema_version": "1.0",
                "job_id": task_context["job_id"],
                "task_id": task_context["task_id"],
                "trace_id": task_context.get("trace_id"),
                "worker_id": self.worker_id,
                "datasource_id": task_context["datasource_id"],
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "payload_record_count": None,
                "message_sequence": 1,  # Status update is always single message
                "is_final_message": True,
                "total_messages_in_task": 1
            },
            "payload": None,  # No primary payload for status update
            "TaskOutputRecords": None,  # No TORs for status update
            "TaskCompletionStatus": status_payload.model_dump()
        }
        
        # Send to Kafka
        # This uses the underlying AIOKafkaProducer directly, not upload_file_to_kafka
        if not self.kafka_producer.is_available():
            raise NetworkError(
                "Kafka producer not available for status update",
                ErrorType.NETWORK_CONNECTION_FAILED
            )
        
        await self.kafka_producer._send_message(
            topic=self.kafka_producer.config.topics.worker_results,  # prod_worker_results
            message=message,
            partition_key=task_context["datasource_id"],
            context=task_context
        )
        
        self.logger.info(
            "Sent TASK_STATUS_UPDATE to Kafka",
            status=status_payload.status,
            **task_context
        )



    async def _report_task_completion_async(
        self,
        task_id: str,
        status: str,
        result_payload: Dict[str, Any]
    ):
        """
        MODIFIED: Use backend abstraction instead of if/else for mode detection.
        
        Old code (lines 1202-1235) REPLACED with single backend call.
        """
        context = {"worker_id": self.worker_id, "task_id": task_id}
        
        try:
            await self.backend.report_task_completion(
                task_id=task_id,
                status=status,
                result_payload=result_payload,
                context=context
            )
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "report_task_completion_async",
                operation="completion_reporting",
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.error(
                "Failed to report task completion",
                error_id=error.error_id
            )
            # Re-raise - this should fail the task
            raise


    async def _send_heartbeat_async(self, task_id: str):
        """
        MODIFIED: Use backend abstraction.
        
        Old code (lines 1237-1263) REPLACED with single backend call.
        """
        self.last_heartbeat = datetime.now(timezone.utc)
        context = {"worker_id": self.worker_id, "task_id": task_id}
        
        try:
            await self.backend.send_heartbeat(
                worker_id=self.worker_id,
                task_id=task_id,
                context=context
            )
            self.logger.log_heartbeat(task_id, self.worker_id)
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "send_heartbeat_async",
                operation="heartbeat_reporting",
                task_id=task_id,
                worker_id=self.worker_id
            )
            self.logger.warning(
                "Failed to send heartbeat",
                error_id=error.error_id
            )
            # Don't raise - heartbeat is best-effort


# =============================================================================
# Factory Classes for Dependency Injection
# =============================================================================

class ConnectorFactory:
    """Factory for creating connector instances with interface detection."""
    
    def __init__(self, logger: SystemLogger, error_handler: ErrorHandler, 
                 db_interface: DatabaseInterface, credential_manager: Any, system_config: Any):
        self.logger = logger
        self.error_handler = error_handler
        self.db_interface = db_interface
        self.credential_manager = credential_manager
        self.system_config = system_config


    async def create_connector(self, datasource_id: str) -> Union[IFileDataSourceConnector, IDatabaseDataSourceConnector]:
            """
            Fetches a datasource's configuration and creates the correct
            connector instance on-demand.
            """
            # 1. Get the full configuration for the requested data source
            datasource_config = await self.db_interface.get_datasource_configuration(datasource_id)
            if not datasource_config:
                raise ValueError(f"Configuration for datasource '{datasource_id}' not found.")
            
            ds_type = datasource_config.datasource_type.lower()
            
            self.logger.info(f"Creating connector for datasource '{datasource_id}' of type '{ds_type}'.")

            # 2. Use the 'datasource_type' to decide which connector class to instantiate
            if ds_type == 'smb':
                from connectors.smb_connector import SMBConnector
                return SMBConnector(
                    datasource_id=datasource_id,
                    smb_config=datasource_config.configuration,
                    system_config=self.system_config,
                    logger=self.logger,
                    error_handler=self.error_handler,
                    db_interface=self.db_interface,
                    credential_manager=self.credential_manager
                )
            elif ds_type == 'local':
                from connectors.local_connector import LocalConnector
                return LocalConnector(
                    datasource_id=datasource_id,
                    local_config=datasource_config.configuration,
                    system_config=self.system_config,
                    logger=self.logger,
                    error_handler=self.error_handler
                )
            elif ds_type == 'sqlserver':
                from connectors.sql_server_connector import SQLServerConnector
                return SQLServerConnector(
                    datasource_id=datasource_id,
                    logger=self.logger,
                    error_handler=self.error_handler,
                    db_interface=self.db_interface,
                    credential_manager=self.credential_manager
                )
            else:
                raise ValueError(f"Unknown or unsupported connector type: '{ds_type}'")

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