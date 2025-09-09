# src/orchestrator/orchestrator.py
"""
The main Orchestrator class. Manages shared state, component lifecycle,
and provides the public interface for the API and CLI.

ASYNC CONVERSION (CORRECTED):
- Replaced all queue.Queue with asyncio.Queue
- Converted background threads to async coroutines  
- Fixed _InProcessWorker to be async-compatible
- Ensured pure async architecture throughout
- All queue operations now use await
"""

import sys
import asyncio
import time
from collections import deque
from typing import Dict, Any, Optional, List
from enum import Enum

# Handle select import with fallback
try:
    import select
    HAS_SELECT = hasattr(select, 'select')
except ImportError:
    HAS_SELECT = False

from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.db.database_interface import DatabaseInterface
from orchestrator.resource_coordinator import ResourceCoordinator
from core.db_models.job_schema import Task, JobStatus
from core.errors import ErrorHandler, ClassificationError

# Import the refactored async coroutine components
from .threads.task_assigner import TaskAssigner
from .threads.reaper import Reaper
from .threads.pipeliner import Pipeliner
from .threads.job_monitor import JobCompletionMonitor
from core.db_models.job_schema import Task, JobStatus, Job

from core.config.configuration_manager import ClassificationConfidenceConfig

class JobState(str, Enum):
    """Valid job states for state machine validation"""
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Orchestrator:
    """Manages jobs, tasks, workers, and system state for all deployment models."""

    # Valid state transitions
    VALID_TRANSITIONS = {
        JobState.QUEUED: [JobState.RUNNING, JobState.CANCELLED],
        JobState.RUNNING: [JobState.PAUSING, JobState.CANCELLING, JobState.COMPLETED, JobState.FAILED],
        JobState.PAUSING: [JobState.PAUSED, JobState.CANCELLING],
        JobState.PAUSED: [JobState.RUNNING, JobState.CANCELLING],
        JobState.CANCELLING: [JobState.CANCELLED],
        JobState.CANCELLED: [],  # Terminal state
        JobState.COMPLETED: [],  # Terminal state
        JobState.FAILED: [],     # Terminal state
    }

    def __init__(self, settings: SystemConfig, db: DatabaseInterface, logger: SystemLogger, error_handler: ErrorHandler):
        self.settings = settings
        self.db = db
        self.logger = logger
        self.error_handler = error_handler
        
        # ASYNC CONVERSION: Replace threading.Event with asyncio.Event
        self._shutdown_event = asyncio.Event()
        
        # --- Shared State (with async locks) ---
        self.job_states: Dict[int, JobState] = {}
        self.job_cache: Dict[int, deque] = {}
        self.snoozed_jobs: Dict[int, float] = {}
        self.thread_liveness: Dict[str, float] = {}
        
        # ASYNC CONVERSION: Replace threading.RLock with asyncio.Lock
        self._state_lock = asyncio.Lock()
        
        # Cache cleanup tracking
        self._cache_cleanup_counter = 0
        self._CACHE_CLEANUP_INTERVAL = 100

        # --- Mode-Specific Components ---
        self.is_single_process_mode = self.settings.orchestrator.deployment_model.upper() == "SINGLE_PROCESS"
        
        # FIXED: Replace queue.Queue with asyncio.Queue for async operations
        self.waiting_workers = asyncio.Queue()
        self._in_process_work_queue = asyncio.Queue()
        self._worker_tasks: List[asyncio.Task] = []
        
        # --- Async Coroutine Tasks ---
        self._background_tasks: List[asyncio.Task] = []

        # --- Internal Components ---
        try:
            self.resource_coordinator = ResourceCoordinator(settings, logger, db)
            self.task_assigner = TaskAssigner(self)
            self.reaper = Reaper(self)
            self.pipeliner = Pipeliner(self)
            self.job_monitor = JobCompletionMonitor(self)
            self.cli_processor = self._CliCommandProcessor(self)
            
            self.logger.log_component_lifecycle("Orchestrator", "INITIALIZED")
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "orchestrator_initialization", 
                operation="component_initialization"
            )
            self.logger.error("Failed to initialize Orchestrator components", error_id=error.error_id)
            raise

    async def start_async(self):
        """Starts all background coroutines in async event loop."""
        try:
            self.logger.log_component_lifecycle("Orchestrator", "STARTING_ASYNC_COROUTINES")
            
            if self.is_single_process_mode:
                await self._start_in_process_workers_async()
            
            # Start all background coroutines
            coroutines_to_start = [
                self.task_assigner.run_async(),
                self.reaper.run_async(), 
                self.pipeliner.run_async(),
                self.job_monitor.run_async()
            ]
            
            # Create tasks for all coroutines
            self._background_tasks = [
                asyncio.create_task(coroutine, name=f"orchestrator_{i}")
                for i, coroutine in enumerate(coroutines_to_start)
            ]
            
            for task in self._background_tasks:
                self.logger.info(f"Started async coroutine: {task.get_name()}")
            
            if self.settings.orchestrator.cli_enabled:
                # CLI processor remains as thread for now (keyboard input is inherently blocking)
                asyncio.create_task(self._start_cli_processor())
                self.logger.info("Started CLI processor")
                
            self.logger.log_component_lifecycle("Orchestrator", "ALL_ASYNC_COROUTINES_STARTED")
            
            # Wait for all background tasks to complete (or until shutdown)
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "orchestrator_async_startup",
                operation="start_background_coroutines"
            )
            self.logger.error("Failed to start Orchestrator async coroutines", error_id=error.error_id)
            raise

    async def shutdown_async(self):
        """Async shutdown method for proper async cleanup."""
        try:
            self.logger.log_component_lifecycle("Orchestrator", "SHUTTING_DOWN_ASYNC")
            
            # Set shutdown event
            self._shutdown_event.set()
            
            # Shutdown workers
            await self._shutdown_workers_async()
            
            # Cancel and wait for background async tasks
            if self._background_tasks:
                for task in self._background_tasks:
                    if not task.done():
                        task.cancel()
                
                # Wait for tasks to complete cancellation
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            
            self.logger.log_component_lifecycle("Orchestrator", "ASYNC_SHUTDOWN_COMPLETE")
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "orchestrator_async_shutdown",
                operation="async_graceful_shutdown"
            )
            self.logger.error("Error during Orchestrator async shutdown", error_id=error.error_id)

    async def _shutdown_workers_async(self):
        """Helper method to shutdown async workers."""
        if self.is_single_process_mode:
            # Send shutdown signal to all worker coroutines
            for _ in self._worker_tasks: 
                await self._in_process_work_queue.put(None)
            
            # Cancel and wait for all worker tasks
            if self._worker_tasks:
                for worker_task in self._worker_tasks:
                    if not worker_task.done():
                        worker_task.cancel()
                
                await asyncio.gather(*self._worker_tasks, return_exceptions=True)
                
            self.logger.info(f"Shutdown {len(self._worker_tasks)} worker coroutines")
        else:
            # Signal waiting workers in EKS mode
            try:
                await asyncio.wait_for(self.waiting_workers.put(None), timeout=1.0)
            except asyncio.TimeoutError:
                self.logger.warning("Could not signal shutdown to waiting workers - queue full")

    async def _start_in_process_workers_async(self):
        """Creates and starts async worker coroutines for single-process mode."""
        thread_count = self.settings.worker.in_process_thread_count
        
        if not isinstance(thread_count, int) or thread_count <= 0:
            self.logger.warning(
                "in_process_thread_count is invalid; no workers will be started.",
                thread_count=thread_count
            )
            return
            
        if thread_count > 50:  # Safety check
            self.logger.warning(
                "in_process_thread_count is very high, limiting to 50 workers",
                requested=thread_count
            )
            thread_count = 50
            
        # FIXED: Create async worker coroutines instead of threads
        for i in range(thread_count):
            worker_id = f"in-process-worker-{i+1}"
            worker_task = asyncio.create_task(
                self._worker_coroutine(worker_id), 
                name=worker_id
            )
            self._worker_tasks.append(worker_task)
            
        self.logger.info(f"Started {thread_count} in-process worker coroutines")

    async def _worker_coroutine(self, worker_id: str):
        """Async worker coroutine for single-process mode."""
        self.logger.log_component_lifecycle(worker_id, "STARTED")
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    # FIXED: Use await with asyncio.Queue instead of blocking get
                    task = await asyncio.wait_for(
                        self._in_process_work_queue.get(), 
                        timeout=1.0
                    )
                    
                    if task is None:  # Shutdown signal
                        break
                        
                    self.logger.info(
                        f"{worker_id} starting task {task.ID}",
                        task_id=task.ID,
                        worker_id=worker_id
                    )
                    
                    # TODO: Replace this simulation with actual task processing
                    # This would integrate with the connector and classification systems
                    await asyncio.sleep(2)  # Simulate work
                    
                    # Report successful completion
                    await self.report_task_result(
                        task.ID, 
                        "COMPLETED", 
                        False,
                        {"simulation": True},
                        {"worker_id": worker_id}
                    )
                    
                except asyncio.TimeoutError:
                    continue  # Check shutdown event
                except Exception as task_error:
                    # Report task failure
                    if 'task' in locals():
                        await self.report_task_result(
                            task.ID,
                            "FAILED",
                            True,  # Retryable
                            {"error": str(task_error)},
                            {"worker_id": worker_id}
                        )
                    
                    error = self.error_handler.handle_error(
                        task_error,
                        "in_process_worker_task_execution",
                        operation="task_processing",
                        worker_id=worker_id
                    )
                    self.logger.error("Worker task execution failed", error_id=error.error_id)
                    
        except Exception as worker_error:
            error = self.error_handler.handle_error(
                worker_error,
                "in_process_worker_main_loop",
                operation="worker_main_loop",
                worker_id=worker_id
            )
            self.logger.error("Worker coroutine crashed", error_id=error.error_id)
        finally:
            self.logger.log_component_lifecycle(worker_id, "STOPPED")

    def update_thread_liveness(self, thread_name: str):
        """Allows coroutines to report that they are alive and functioning."""
        self.thread_liveness[thread_name] = time.monotonic()

    def _validate_state_transition(self, job_id: int, from_state: JobState, to_state: JobState) -> bool:
        """Validates if a job state transition is allowed."""
        valid_targets = self.VALID_TRANSITIONS.get(from_state, [])
        if to_state not in valid_targets:
            self.logger.warning(
                f"Invalid state transition for job {job_id}: {from_state.value} -> {to_state.value}",
                job_id=job_id,
                from_state=from_state.value,
                to_state=to_state.value,
                valid_targets=[s.value for s in valid_targets]
            )
            return False
        return True

    async def _safe_update_job_status(self, job_id: int, status: JobState) -> bool:
        """Safely update job status with proper type conversion and error handling."""
        try:
            # Validate that the status exists in JobStatus enum
            if hasattr(JobStatus, status.value):
                db_status = getattr(JobStatus, status.value)
                await self.db.update_job_status(job_id, db_status)
                return True
            else:
                self.logger.error(f"Invalid JobStatus value: {status.value}", job_id=job_id)
                return False
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "safe_update_job_status",
                operation="database_status_update",
                job_id=job_id,
                status=status.value
            )
            self.logger.error("Failed to update job status in database", error_id=error.error_id)
            return False

    # Integration compatibility methods for TaskAssigner
    def release_task_reservation(self, datasource_id: str):
        """Compatibility method for TaskAssigner integration."""
        self.resource_coordinator.state_manager.release_datasource_connection(datasource_id)
        
    def confirm_task_assignment(self, job_id: int):
        """Compatibility method for TaskAssigner integration."""  
        self.resource_coordinator.state_manager.confirm_task_assignment(job_id)

    async def _cleanup_job_cache_if_needed(self):
        """Periodically clean up job cache to prevent memory leaks."""
        self._cache_cleanup_counter += 1
        if self._cache_cleanup_counter >= self._CACHE_CLEANUP_INTERVAL:
            async with self._state_lock:
                terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
                jobs_to_remove = [
                    job_id for job_id, state in self.job_states.items()
                    if state in terminal_states and job_id in self.job_cache
                ]
                
                for job_id in jobs_to_remove:
                    del self.job_cache[job_id]
                    
                if jobs_to_remove:
                    self.logger.info(f"Cleaned up cache for {len(jobs_to_remove)} completed jobs")
                    
            self._cache_cleanup_counter = 0

    # =================================================================
    # Public Methods for Worker Communication (API Layer)
    # =================================================================

    async def get_task_async(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Long-polling endpoint for workers to get tasks (async version)."""
        if self.is_single_process_mode: 
            return None
            
        try:
            # FIXED: Use asyncio.Queue instead of sync queue
            await self.waiting_workers.put((worker_id, None))  # Simplified for now
            # In full implementation, would use proper response queues
            return None
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "get_task_endpoint_async",
                operation="worker_task_request",
                worker_id=worker_id
            )
            self.logger.error("Error in get_task async endpoint", error_id=error.error_id)
            return None

    async def report_task_result(self, task_id: int, status: str, is_retryable: bool, 
                                result_payload: Optional[Dict] = None, context: Optional[Dict] = None):
        """Reports the final result of a task."""
        context = context or {}
        result_payload = result_payload or {}
        
        try:
            task = await self.db.get_task_by_id(task_id, context=context)
            if not task:
                self.logger.warning(
                    "Received result for an unknown or already processed task.", 
                    task_id=task_id
                )
                return
            
            # Update task in database
            if status.upper() == "COMPLETED":
                await self.db.complete_task(task_id, context=context)
                self.logger.info(f"Task {task_id} completed successfully", task_id=task_id)
            else:
                await self.db.fail_task(task_id, is_retryable, context=context)
                self.logger.warning(f"Task {task_id} failed", task_id=task_id, is_retryable=is_retryable)
            
            # Release resources
            self.release_resources_for_task(task, context)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "report_task_result",
                operation="task_result_processing",
                task_id=task_id,
                status=status
            )
            self.logger.error("Error processing task result", error_id=error.error_id)

    async def update_task_progress(self, task_id: int, progress_record: Dict, 
                                  context: Optional[Dict] = None) -> None:
        """Reports intermediate progress from a running task."""
        context = context or {}
        
        try:
            await self.db.write_task_output_record(
                task_id=task_id,
                output_type=progress_record.get("OutputType", "UNKNOWN"),
                payload=progress_record.get("OutputPayload", {}),
                context=context
            )
            
            # Log with sampling to avoid spam
            self.logger.log_progress_batch(
                progress_record.get("OutputPayload", {}),
                task_id,
                sampling_rate=0.1  # Log only 10% of progress updates
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "update_task_progress",
                operation="progress_update",
                task_id=task_id
            )
            self.logger.error("Error updating task progress", error_id=error.error_id)

    async def report_heartbeat(self, task_id: int, context: Optional[Dict] = None) -> None:
        """Extends the lease for a running task."""
        context = context or {}
        
        try:
            success = await self.db.extend_task_lease(
                task_id, 
                self.settings.worker.task_timeout_seconds, 
                context=context
            )
            
            if not success:
                self.logger.warning(
                    f"Heartbeat failed for task {task_id} - task may no longer be assigned",
                    task_id=task_id
                )
            else:
                self.logger.log_heartbeat(task_id, f"worker-{task_id}")
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "report_heartbeat",
                operation="heartbeat_processing",
                task_id=task_id
            )
            self.logger.error("Error processing heartbeat", error_id=error.error_id)

    # =================================================================
    # Public Methods for Job Control (API/CLI) - All Async
    # =================================================================


    async def start_job_from_template(self, template_id: str, job_type: JobType, trigger_type: str = "manual") -> Optional[int]:
        """
        Creates and starts a job from either a ScanTemplate or a PolicyTemplate.
        """
        try:
            self.logger.info(f"Attempting to start job from template '{template_id}'", template_id=template_id, job_type=job_type.value)
            
            # UPDATED: Fetch the correct template type based on the job_type
            template = None
            if job_type == JobType.SCANNING:
                template = await self.db.get_scan_template_by_id(template_id)
            elif job_type == JobType.POLICY:
                template = await self.db.get_policy_template_by_id(template_id)

            if not template:
                self.logger.error(f"Template '{template_id}' of type '{job_type.value}' not found", template_id=template_id)
                return None

            # Create job execution using the updated, polymorphic database method
            execution_id = f"run-{template_id}-{int(time.time())}"
            job = await self.db.create_job_execution(template_id, job_type, execution_id, trigger_type)
            
            # Validate and transition to RUNNING state
            async with self._state_lock:
                self.job_states[job.id] = JobState.RUNNING
            
            if not await self._safe_update_job_status(job.id, JobState.RUNNING):
                async with self._state_lock:
                    self.job_states[job.id] = JobState.QUEUED  # Rollback state
                return None
            
            # NEW LOGIC: Create different initial tasks based on the job type
            task_count = 0
            if job_type == JobType.SCANNING:
                # This is the original logic for scanning jobs
                for target in template.configuration.get("datasource_targets", []):
                    await self.db.create_task(
                        job_id=job.id,
                        task_type="DISCOVERY_ENUMERATE",
                        work_packet={"payload": {"paths": target.get("paths", [])}},
                        datasource_id=target.get("datasource_id")
                    )
                    task_count += 1
            
            elif job_type == JobType.POLICY:
                # This is the new logic for the first step of a policy job
                plan_id = f"plan_for_job_{job.id}"
                policy_config = PolicyConfiguration(**template.configuration)

                payload = PolicySelectorPlanPayload(
                    plan_id=plan_id,
                    policy_config=policy_config
                )
                
                await self.db.create_task(
                    job_id=job.id,
                    task_type=TaskType.POLICY_SELECTOR_PLAN,
                    work_packet={"payload": payload.dict()}
                )
                task_count = 1

            if task_count == 0:
                self.logger.error(f"No initial tasks were created for job {job.id}. Marking as failed.", job_id=job.id)
                await self.finalize_job_state_async(job.id, JobState.FAILED)
                return None
            
            self.logger.info(
                f"Successfully started job {job.id} with {task_count} initial tasks",
                job_id=job.id,
                template_id=template_id,
                task_count=task_count
            )
            return job.id
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "start_job_from_template",
                operation="job_creation",
                template_id=template_id
            )
            self.logger.error("Failed to start job from template", error_id=error.error_id)
            return None



    async def finalize_job_state_async(self, job_id: int, final_state: JobState):
        """Finalizes a job's state and cleans up all associated resources."""
        try:
            current_state = None
            
            async with self._state_lock:
                current_state = self.job_states.get(job_id, JobState.QUEUED)
                
                if current_state == final_state:
                    return  # Already in target state
                
                # Special handling for terminal states - allow direct transition
                terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
                if final_state in terminal_states:
                    # For terminal states, validate transition or allow from CANCELLING state
                    if (current_state == JobState.CANCELLING and final_state == JobState.CANCELLED) or \
                       self._validate_state_transition(job_id, current_state, final_state):
                        self.job_states[job_id] = final_state
                    else:
                        self.logger.error(
                            f"Invalid final state transition for job {job_id}: {current_state.value} -> {final_state.value}"
                        )
                        return
                else:
                    if not self._validate_state_transition(job_id, current_state, final_state):
                        self.logger.error(
                            f"Invalid final state transition for job {job_id}: {current_state.value} -> {final_state.value}"
                        )
                        return
                    self.job_states[job_id] = final_state
                
                self.logger.info(
                    f"Finalizing job {job_id} with status {final_state.value}",
                    job_id=job_id,
                    final_status=final_state.value,
                    previous_status=current_state.value
                )
                
                # Clean up all caches and temporary data (while holding lock)
                self.job_cache.pop(job_id, None)
                self.snoozed_jobs.pop(job_id, None)
            
            # Database update
            await self._safe_update_job_status(job_id, final_state)
            
            self.logger.info(f"Job {job_id} finalized successfully", job_id=job_id)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "finalize_job_state_async",
                operation="job_finalization",
                job_id=job_id,
                final_state=final_state.value
            )
            self.logger.error("Failed to finalize job state", error_id=error.error_id)

    def release_resources_for_task(self, task: Task, context: Optional[Dict] = None):
        """Releases all resources associated with a task."""
        try:
            context = context or {}
            
            # Release worker slot
            self.resource_coordinator.state_manager.release_job_worker_slot(task.JobID)
            
            # Release datasource connection if applicable
            if task.DatasourceID:
                self.resource_coordinator.state_manager.release_datasource_connection(task.DatasourceID)
            
            self.logger.info(
                f"Released resources for task {task.ID}",
                task_id=task.ID,
                job_id=task.JobID,
                datasource_id=task.DatasourceID
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "release_resources_for_task",
                operation="resource_release",
                task_id=task.ID,
                job_id=task.JobID
            )
            self.logger.error("Failed to release task resources", error_id=error.error_id)

    # Sync compatibility method for external callers
    def finalize_job_state(self, job_id: int, final_state: JobState):
        """Sync wrapper for finalize_job_state_async (for compatibility)."""
        asyncio.create_task(self.finalize_job_state_async(job_id, final_state))

    async def _start_cli_processor(self):
        """Start CLI processor as async task."""
        # CLI would need to be converted to async as well for full integration
        # For now, keeping minimal placeholder
        pass

    class _CliCommandProcessor:
        """CLI command processor placeholder."""
        
        def __init__(self, parent: 'Orchestrator'):
            self.parent = parent