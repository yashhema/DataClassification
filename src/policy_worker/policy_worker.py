import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import (
    ErrorHandler, ClassificationError, NetworkError, 
    ProcessingError, ErrorType, ErrorSeverity
)
from core.models.models import WorkPacket, Task
from core.interfaces.api_backend import APIBackend

from .config import PolicyWorkerConfig
from .handlers.policy_query_handler import PolicyQueryHandler
from .handlers.prepare_classification_handler import PrepareClassificationHandler
from .handlers.job_closure_handler import JobClosureHandler
from .repositories.policy_worker_repository import PolicyWorkerRepository
from .services.query_executor import QueryExecutor
from .services.task_generator import TaskGenerator
from .services.merge_coordinator import MergeCoordinator
from .services.s3_promoter import S3Promoter

class PolicyWorker:
    """
    Central worker for policy execution, classification preparation, and job closure.
    Operates in standalone mode (polling API) or single-process mode (in-process queue).
    """
    
    def __init__(
        self,
        config: PolicyWorkerConfig,
        db_repository: PolicyWorkerRepository,
        logger: SystemLogger,
        error_handler: ErrorHandler,
        backend: Optional[APIBackend] = None,
        orchestrator_instance: Optional[Any] = None
    ):
        self.config = config
        self.db = db_repository
        self.logger = logger
        self.error_handler = error_handler
        self.backend = backend
        self.orchestrator = orchestrator_instance
        
        # Determine backend based on deployment mode
        if config.deployment_mode == "single_process":
            if not orchestrator_instance:
                raise ValueError("orchestrator_instance required for single_process mode")
            self._in_process_mode = True
        else:
            if not backend:
                raise ValueError("backend required for standalone mode")
            self._in_process_mode = False
        
        # Initialize handlers (dependency injection)
        self.policy_query_handler = PolicyQueryHandler(self)
        self.prepare_classification_handler = PrepareClassificationHandler(self)
        self.job_closure_handler = JobClosureHandler(self)
        
        # Initialize services
        self.query_executor = QueryExecutor(self)
        self.task_generator = TaskGenerator(self)
        self.merge_coordinator = MergeCoordinator(self)
        self.s3_promoter = S3Promoter(self)
        
        # State
        self._running = False
        self._current_task: Optional[Task] = None
        
        self.logger.log_component_lifecycle(
            component=config.component_name,
            event="initialized",
            deployment_mode=config.deployment_mode,
            worker_id=config.worker_id
        )
    
    async def start(self):
        """Start PolicyWorker main loop"""
        self._running = True
        
        self.logger.log_component_lifecycle(
            component=self.config.component_name,
            event="starting",
            worker_id=self.config.worker_id
        )
        
        try:
            # Register with system
            await self._register_component()
            
            # Start main loop
            if self._in_process_mode:
                await self._run_in_process_mode()
            else:
                await self._run_standalone_mode()
        
        except Exception as e:
            error = ProcessingError(
                message="PolicyWorker failed during startup",
                error_type=ErrorType.SYSTEM_INTERNAL_ERROR,
                severity=ErrorSeverity.CRITICAL,
                context={"worker_id": self.config.worker_id},
                cause=e
            )
            self.logger.log_classification_error(error, trace_id=None)
            raise
    
    async def stop(self):
        """Gracefully stop PolicyWorker"""
        self.logger.log_component_lifecycle(
            component=self.config.component_name,
            event="stopping",
            worker_id=self.config.worker_id
        )
        
        self._running = False
        
        # Wait for current task to complete (with timeout)
        if self._current_task:
            self.logger.info(
                "Waiting for current task to complete",
                task_id=self._current_task.id.hex()
            )
            await asyncio.sleep(5)  # Grace period
    
    async def _register_component(self):
        """Register PolicyWorker with component registry"""
        try:
            if self._in_process_mode:
                # Direct registration via Orchestrator
                await self.orchestrator.register_component_async(
                    component_id=self.config.worker_id,
                    component_type="PolicyWorker",
                    worker_type="POLICY",
                    node_group=self.config.node_group
                )
            else:
                # API registration
                await self.backend.register_worker(
                    worker_id=self.config.worker_id,
                    worker_type="POLICY",
                    capabilities=["policy_query", "prepare_classification", "job_closure"]
                )
            
            self.logger.info(
                "PolicyWorker registered successfully",
                worker_id=self.config.worker_id,
                worker_type="POLICY"
            )
        
        except Exception as e:
            error = NetworkError(
                message="Failed to register PolicyWorker",
                error_type=ErrorType.NETWORK_CONNECTION_FAILED,
                severity=ErrorSeverity.HIGH,
                retryable=True,
                context={"worker_id": self.config.worker_id},
                cause=e
            )
            self.logger.log_classification_error(error, trace_id=None)
            raise
    
    async def _run_in_process_mode(self):
        """Poll in-process queue for tasks (single-process deployment)"""
        self.logger.info("PolicyWorker running in single-process mode")
        
        while self._running:
            try:
                # Get task from Orchestrator's internal queue
                task = await self.orchestrator.get_task_async(
                    worker_id=self.config.worker_id,
                    worker_type="POLICY"
                )
                
                if task:
                    await self._process_task(task)
                else:
                    await asyncio.sleep(self.config.task_poll_interval_seconds)
            
            except Exception as e:
                error = ProcessingError(
                    message="Error in PolicyWorker main loop",
                    error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                    severity=ErrorSeverity.HIGH,
                    context={"worker_id": self.config.worker_id},
                    cause=e
                )
                self.logger.log_classification_error(error, trace_id=None)
                await asyncio.sleep(5)  # Brief pause before retry
    
    async def _run_standalone_mode(self):
        """Poll TaskAssigner API for tasks (distributed deployment)"""
        self.logger.info("PolicyWorker running in standalone mode")
        
        while self._running:
            try:
                # Poll TaskAssigner API
                task = await self.backend.get_next_task(
                    worker_id=self.config.worker_id,
                    worker_type="POLICY",
                    nodegroup=self.config.node_group
                )
                
                if task:
                    await self._process_task(task)
                else:
                    await asyncio.sleep(self.config.task_poll_interval_seconds)
            
            except NetworkError as e:
                if e.retryable:
                    self.logger.warning(
                        "Transient network error polling for tasks",
                        error_id=e.error_id
                    )
                    await asyncio.sleep(10)
                else:
                    self.logger.log_classification_error(e, trace_id=None)
                    raise
            
            except Exception as e:
                error = ProcessingError(
                    message="Error in PolicyWorker main loop",
                    error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                    severity=ErrorSeverity.HIGH,
                    context={"worker_id": self.config.worker_id},
                    cause=e
                )
                self.logger.log_classification_error(error, trace_id=None)
                await asyncio.sleep(5)
    
    async def _process_task(self, task: Task):
        """Route task to appropriate handler with error handling"""
        self._current_task = task
        
        try:
            # Parse WorkPacket
            work_packet = WorkPacket.model_validate(task.work_packet)
            trace_id = work_packet.header.trace_id
            
            self.logger.log_task_assignment(
                task_id=task.id,
                worker_id=self.config.worker_id,
                trace_id=trace_id,
                task_type=task.task_type
            )
            
            # Route to handler
            if task.task_type == "POLICY_QUERY_EXECUTE":
                await self.policy_query_handler.process(work_packet)
            
            elif task.task_type == "PREPARE_CLASSIFICATION_TASKS":
                await self.prepare_classification_handler.process(work_packet)
            
            elif task.task_type == "JOB_CLOSURE":
                await self.job_closure_handler.process(work_packet)
            
            else:
                raise ProcessingError(
                    message=f"Unknown task type: {task.task_type}",
                    error_type=ErrorType.PROCESSING_LOGIC_ERROR,
                    severity=ErrorSeverity.MEDIUM,
                    context={"task_id": task.id.hex(), "task_type": task.task_type}
                )
            
            # Report completion
            await self._report_task_completion(
                task_id=task.id,
                status="COMPLETED",
                trace_id=trace_id
            )
            
            self.logger.log_task_completion(
                task_id=task.id,
                status="COMPLETED",
                trace_id=trace_id
            )
        
        except ClassificationError as e:
            # Structured error - log and report
            self.logger.log_classification_error(e, trace_id=work_packet.header.trace_id)
            
            await self._report_task_completion(
                task_id=task.id,
                status="FAILED",
                trace_id=work_packet.header.trace_id,
                error_id=e.error_id,
                error_message=e.message
            )
        
        except Exception as e:
            # Unexpected error - wrap and report
            error = ProcessingError(
                message=f"Unexpected error processing task: {str(e)}",
                error_type=ErrorType.SYSTEM_INTERNAL_ERROR,
                severity=ErrorSeverity.HIGH,
                context={"task_id": task.id.hex()},
                cause=e
            )
            self.logger.log_classification_error(error, trace_id=work_packet.header.trace_id)
            
            await self._report_task_completion(
                task_id=task.id,
                status="FAILED",
                trace_id=work_packet.header.trace_id,
                error_id=error.error_id,
                error_message=error.message
            )
        
        finally:
            self._current_task = None
    
    async def _report_task_completion(
        self,
        task_id: bytes,
        status: str,
        trace_id: str,
        error_id: Optional[str] = None,
        error_message: Optional[str] = None
    ):
        """Report task completion to Orchestrator/Backend"""
        try:
            if self._in_process_mode:
                await self.orchestrator.report_task_completion_async(
                    task_id=task_id,
                    status=status,
                    error_id=error_id,
                    error_message=error_message
                )
            else:
                await self.backend.report_task_completion(
                    task_id=task_id.hex(),
                    status=status,
                    result_payload={
                        "trace_id": trace_id,
                        "error_id": error_id,
                        "error_message": error_message
                    }
                )
        
        except Exception as e:
            self.logger.error(
                "Failed to report task completion",
                task_id=task_id.hex(),
                status=status,
                error=str(e)
            )
