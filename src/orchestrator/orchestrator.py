# src/orchestrator/orchestrator.py
"""
The main Orchestrator class. Manages shared state, component lifecycle,
and provides the public interface for the API and CLI.

FIXES APPLIED (V2):
- Fixed ResourceCoordinator constructor signature
- Replaced print statements with proper logging  
- Added non-blocking CLI implementation with proper import handling
- Fixed race conditions with consistent lock usage
- Reduced lock scope to prevent deadlocks
- Added memory leak prevention in job cache
- Added proper state transition validation with consistency
- Enhanced error handling and context propagation
- Added comprehensive health checks
- Fixed type safety issues with JobStatus enum
- Added integration compatibility methods
"""

import sys
import threading
import time
import queue
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

# Import the refactored thread components
from .threads.task_assigner import TaskAssigner
from .threads.reaper import Reaper
from .threads.pipeliner import Pipeliner
from .threads.job_monitor import JobCompletionMonitor
from core.db_models.job_schema import Task, JobStatus, Job

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
        self._shutdown_event = threading.Event()
        
        # --- Shared State (with locks for thread safety) ---
        self.job_states: Dict[int, JobState] = {}
        self.job_cache: Dict[int, deque] = {}
        self.snoozed_jobs: Dict[int, float] = {}
        self.thread_liveness: Dict[str, float] = {}
        
        # Single lock for all orchestrator state to prevent deadlocks
        self._state_lock = threading.RLock()  # Reentrant lock for nested access
        
        # Cache cleanup tracking
        self._cache_cleanup_counter = 0
        self._CACHE_CLEANUP_INTERVAL = 100  # Clean cache every 100 operations

        # --- Mode-Specific Components ---
        self.is_single_process_mode = self.settings.orchestrator.deployment_model.upper() == "SINGLE_PROCESS"
        self.waiting_workers = queue.Queue()
        self._worker_threads: List['Orchestrator._InProcessWorker'] = []
        self._in_process_work_queue = queue.Queue()

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

    def start(self):
        """Starts all background threads."""
        try:
            self.logger.log_component_lifecycle("Orchestrator", "STARTING_THREADS")
            
            if self.is_single_process_mode:
                self._start_in_process_workers()
            
            # Start all background threads
            threads_to_start = [
                self.task_assigner,
                self.reaper, 
                self.pipeliner,
                self.job_monitor
            ]
            
            for thread in threads_to_start:
                thread.start()
                self.logger.info(f"Started thread: {thread.name}")
            
            if self.settings.orchestrator.cli_enabled:
                self.cli_processor.start()
                self.logger.info("Started CLI processor")
                
            self.logger.log_component_lifecycle("Orchestrator", "ALL_THREADS_STARTED")
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "orchestrator_startup",
                operation="start_background_threads"
            )
            self.logger.error("Failed to start Orchestrator threads", error_id=error.error_id)
            raise

    def shutdown(self):
        """Signals all background threads to shut down gracefully."""
        try:
            self.logger.log_component_lifecycle("Orchestrator", "SHUTTING_DOWN")
            self._shutdown_event.set()
            
            # Shutdown workers first
            if self.is_single_process_mode:
                # Send shutdown signal to all worker threads
                for _ in self._worker_threads: 
                    self._in_process_work_queue.put(None)
                
                # Wait for all workers to finish
                for worker_thread in self._worker_threads:
                    worker_thread.join(timeout=5)
                    if worker_thread.is_alive():
                        self.logger.warning(f"Worker thread {worker_thread.name} did not shutdown gracefully")
            else:
                # Signal waiting workers in EKS mode
                try:
                    self.waiting_workers.put(None, timeout=1)
                except queue.Full:
                    self.logger.warning("Could not signal shutdown to waiting workers - queue full")
            
            # Shutdown background threads
            threads = [self.task_assigner, self.reaper, self.pipeliner, self.job_monitor]
            
            if hasattr(self.cli_processor, 'is_alive') and self.cli_processor.is_alive():
                threads.append(self.cli_processor)
            
            for thread in threads:
                thread.join(timeout=5)
                if thread.is_alive():
                    self.logger.warning(f"Thread {thread.name} did not shutdown gracefully")
            
            self.logger.log_component_lifecycle("Orchestrator", "SHUTDOWN_COMPLETE")
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "orchestrator_shutdown",
                operation="graceful_shutdown"
            )
            self.logger.error("Error during Orchestrator shutdown", error_id=error.error_id)

    def _start_in_process_workers(self):
        """Creates and starts the local worker thread pool for single-process mode."""
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
            
        for i in range(thread_count):
            worker = self._InProcessWorker(self, f"in-process-worker-{i+1}")
            self._worker_threads.append(worker)
            worker.start()
            
        self.logger.info(f"Started {thread_count} in-process worker threads")

    def update_thread_liveness(self, thread_name: str):
        """Allows threads to report that they are alive and functioning."""
        with self._state_lock:
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

    def _safe_update_job_status(self, job_id: int, status: JobState) -> bool:
        """Safely update job status with proper type conversion and error handling."""
        try:
            # Validate that the status exists in JobStatus enum
            if hasattr(JobStatus, status.value):
                db_status = getattr(JobStatus, status.value)
                self.db.update_job_status(job_id, db_status)
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

    def _cleanup_job_cache_if_needed(self):
        """Periodically clean up job cache to prevent memory leaks."""
        self._cache_cleanup_counter += 1
        if self._cache_cleanup_counter >= self._CACHE_CLEANUP_INTERVAL:
            with self._state_lock:
                # Remove caches for jobs that are no longer active
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

    def get_task(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Long-polling endpoint for workers to get tasks."""
        if self.is_single_process_mode: 
            return None
            
        response_queue = queue.Queue(maxsize=1)
        
        try:
            self.waiting_workers.put((worker_id, response_queue), timeout=1)
            task = response_queue.get(timeout=self.settings.orchestrator.long_poll_timeout_seconds)
            return task.WorkPacket if task else None
            
        except (queue.Empty, queue.Full):
            return None
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "get_task_endpoint",
                operation="worker_task_request",
                worker_id=worker_id
            )
            self.logger.error("Error in get_task endpoint", error_id=error.error_id)
            return None

    def report_task_result(self, task_id: int, status: str, is_retryable: bool, 
                          result_payload: Optional[Dict] = None, context: Optional[Dict] = None):
        """Reports the final result of a task."""
        context = context or {}
        result_payload = result_payload or {}
        
        try:
            task = self.db.get_task_by_id(task_id, context=context)
            if not task:
                self.logger.warning(
                    "Received result for an unknown or already processed task.", 
                    task_id=task_id
                )
                return
            
            # Update task in database
            if status.upper() == "COMPLETED":
                self.db.complete_task(task_id, context=context)
                self.logger.info(f"Task {task_id} completed successfully", task_id=task_id)
            else:
                self.db.fail_task(task_id, is_retryable, context=context)
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

    def update_task_progress(self, task_id: int, progress_record: Dict, 
                           context: Optional[Dict] = None) -> None:
        """Reports intermediate progress from a running task."""
        context = context or {}
        
        try:
            self.db.write_task_output_record(
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

    def report_heartbeat(self, task_id: int, context: Optional[Dict] = None) -> None:
        """Extends the lease for a running task."""
        context = context or {}
        
        try:
            success = self.db.extend_task_lease(
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
    # Public Methods for Job Control (API/CLI)
    # =================================================================

    def start_job_from_template(self, template_id: str, trigger_type: str = "manual") -> Optional[int]:
        """Creates and starts a job from a scan template."""
        try:
            self.logger.info(f"Attempting to start job from template '{template_id}'", template_id=template_id)
            
            template = self.db.get_scan_template_by_id(template_id)
            if not template:
                self.logger.error(f"Scan template '{template_id}' not found", template_id=template_id)
                return None

            # Create job execution
            execution_id = f"run-{template_id}-{int(time.time())}"
            job = self.db.create_job_execution(template.id, execution_id, trigger_type)
            
            # Validate and transition to RUNNING state (reduced lock scope)
            initial_state = JobState.QUEUED
            target_state = JobState.RUNNING
            
            with self._state_lock:
                if not self._validate_state_transition(job.id, initial_state, target_state):
                    self.logger.error(f"Cannot start job {job.id} - invalid state transition")
                    return None
                self.job_states[job.id] = target_state
            
            # Database update outside lock to prevent deadlocks
            if not self._safe_update_job_status(job.id, target_state):
                with self._state_lock:
                    self.job_states[job.id] = initial_state  # Rollback
                return None
            
            # Create initial tasks
            task_count = 0
            for target in template.configuration.get("datasource_targets", []):
                try:
                    self.db.create_task(
                        job_id=job.id,
                        task_type="DISCOVERY_ENUMERATE",
                        work_packet={"payload": {"paths": target.get("paths", [])}},
                        datasource_id=target.get("datasource_id")
                    )
                    task_count += 1
                except Exception as task_error:
                    self.logger.warning(
                        f"Failed to create task for datasource {target.get('datasource_id')}",
                        error=str(task_error),
                        job_id=job.id
                    )
            
            if task_count == 0:
                self.logger.error(f"No tasks created for job {job.id} - marking as failed")
                self.finalize_job_state(job.id, JobState.FAILED)
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

    def pause_job(self, job_id: int) -> bool:
        """Pauses a running job."""
        try:
            current_state = None
            target_state = JobState.PAUSING
            
            with self._state_lock:
                current_state = self.job_states.get(job_id, JobState.QUEUED)
                
                if not self._validate_state_transition(job_id, current_state, target_state):
                    self.logger.error(
                        f"Cannot pause job {job_id} from state {current_state.value}",
                        job_id=job_id,
                        current_state=current_state.value
                    )
                    return False
                
                self.job_states[job_id] = target_state
            
            # Database update outside lock
            if not self._safe_update_job_status(job_id, target_state):
                with self._state_lock:
                    self.job_states[job_id] = current_state  # Rollback
                return False
                
            self.logger.info(f"Job {job_id} pause initiated", job_id=job_id)
            return True
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "pause_job",
                operation="job_pause",
                job_id=job_id
            )
            self.logger.error("Failed to pause job", error_id=error.error_id, job_id=job_id)
            return False

    def resume_job(self, job_id: int) -> bool:
        """Resumes a paused job."""
        try:
            current_state = None
            target_state = JobState.RUNNING
            
            with self._state_lock:
                current_state = self.job_states.get(job_id, JobState.QUEUED)
                
                if not self._validate_state_transition(job_id, current_state, target_state):
                    self.logger.error(
                        f"Cannot resume job {job_id} from state {current_state.value}",
                        job_id=job_id,
                        current_state=current_state.value
                    )
                    return False
                
                self.job_states[job_id] = target_state
            
            # Database update outside lock
            if not self._safe_update_job_status(job_id, target_state):
                with self._state_lock:
                    self.job_states[job_id] = current_state  # Rollback
                return False
                
            self.logger.info(f"Job {job_id} resumed", job_id=job_id)
            return True
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "resume_job",
                operation="job_resume",
                job_id=job_id
            )
            self.logger.error("Failed to resume job", error_id=error.error_id, job_id=job_id)
            return False

    def cancel_job(self, job_id: int) -> bool:
        """Cancels a job and all its pending tasks."""
        try:
            current_state = None
            target_state = JobState.CANCELLING
            
            with self._state_lock:
                current_state = self.job_states.get(job_id, JobState.QUEUED)
                
                # Check if already in terminal state
                terminal_states = {JobState.CANCELLED, JobState.COMPLETED, JobState.FAILED}
                if current_state in terminal_states:
                    self.logger.info(f"Job {job_id} is already in terminal state {current_state.value}")
                    return True
                
                if not self._validate_state_transition(job_id, current_state, target_state):
                    self.logger.error(
                        f"Cannot cancel job {job_id} from state {current_state.value}",
                        job_id=job_id,
                        current_state=current_state.value
                    )
                    return False
                
                self.job_states[job_id] = target_state
            
            # Database updates outside lock to avoid deadlocks
            if not self._safe_update_job_status(job_id, target_state):
                with self._state_lock:
                    self.job_states[job_id] = current_state  # Rollback
                return False
        
            # Cancel pending tasks (outside the lock to avoid deadlock)
            cancelled_count = self.db.cancel_pending_tasks_for_job(job_id)
            
            self.logger.info(
                f"Job {job_id} cancellation initiated - cancelled {cancelled_count} pending tasks",
                job_id=job_id,
                cancelled_tasks=cancelled_count
            )
            return True
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "cancel_job",
                operation="job_cancellation",
                job_id=job_id
            )
            self.logger.error("Failed to cancel job", error_id=error.error_id, job_id=job_id)
            return False

    def get_job_statistics(self, job_id: int) -> Dict[str, Any]:
        """Returns current statistics for a job."""
        try:
            progress = self.db.get_job_progress_summary(job_id)
            
            with self._state_lock:
                state = self.job_states.get(job_id, JobState.QUEUED)
                
            return {
                "job_id": job_id,
                "current_state": state.value,
                "task_summary": progress,
                "cache_status": {
                    "has_cache": job_id in self.job_cache,
                    "cache_size": len(self.job_cache.get(job_id, [])) if job_id in self.job_cache else 0
                }
            }
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "get_job_statistics",
                operation="statistics_retrieval",
                job_id=job_id
            )
            self.logger.error("Failed to get job statistics", error_id=error.error_id)
            return {"job_id": job_id, "error": "Failed to retrieve statistics"}

    def get_health(self) -> Dict[str, Any]:
        """Provides comprehensive health status for the Orchestrator."""
        try:
            # Check database connectivity
            db_healthy = self.db.test_database_connection()
            
            # Check thread health
            with self._state_lock:
                now = time.monotonic()
                thread_health = {
                    name: {
                        "healthy": (now - last_seen) < 180,  # 3 minutes threshold
                        "last_seen_seconds_ago": int(now - last_seen)
                    }
                    for name, last_seen in self.thread_liveness.items()
                }
            
            # Check resource coordinator health
            try:
                rc_metrics = self.resource_coordinator.state_manager.get_metrics()
                rc_healthy = True
            except Exception:
                rc_metrics = {}
                rc_healthy = False
            
            # Overall health determination
            unhealthy_threads = [
                name for name, status in thread_health.items() 
                if not status["healthy"]
            ]
            
            overall_healthy = (
                db_healthy and 
                rc_healthy and 
                len(unhealthy_threads) == 0
            )
            
            return {
                "status": "HEALTHY" if overall_healthy else "UNHEALTHY",
                "database_connected": db_healthy,
                "resource_coordinator_healthy": rc_healthy,
                "thread_health": thread_health,
                "unhealthy_threads": unhealthy_threads,
                "resource_metrics": rc_metrics,
                "job_cache_size": len(self.job_cache),
                "active_jobs": len([s for s in self.job_states.values() if s not in {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}])
            }
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "get_health",
                operation="health_check"
            )
            return {
                "status": "ERROR",
                "error": f"Health check failed: {error.error_id}"
            }

    def finalize_job_state(self, job_id: int, final_state: JobState):
        """Finalizes a job's state and cleans up all associated resources."""
        try:
            current_state = None
            
            with self._state_lock:
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
            
            # Database update outside lock
            self._safe_update_job_status(job_id, final_state)
            
            self.logger.info(f"Job {job_id} finalized successfully", job_id=job_id)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "finalize_job_state",
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
            
            # Trigger cache cleanup check
            self._cleanup_job_cache_if_needed()
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e, "release_resources_for_task",
                operation="resource_release",
                task_id=task.ID,
                job_id=task.JobID
            )
            self.logger.error("Failed to release task resources", error_id=error.error_id)

    # =================================================================
    # Internal Nested Classes for Threads
    # =================================================================

    class _InProcessWorker(threading.Thread):
        """Internal worker thread for single-process mode."""
        
        def __init__(self, parent: 'Orchestrator', worker_id: str):
            super().__init__(name=worker_id, daemon=True)
            self.parent = parent
            self.worker_id = worker_id
            self.logger = parent.logger

        def run(self):
            """Main worker loop."""
            self.logger.log_component_lifecycle(self.worker_id, "STARTED")
            
            try:
                while not self.parent._shutdown_event.is_set():
                    try:
                        # Get task with timeout to allow shutdown checks
                        task = self.parent._in_process_work_queue.get(timeout=1)
                        
                        if task is None:  # Shutdown signal
                            break
                            
                        self.logger.info(
                            f"{self.worker_id} starting task {task.ID}",
                            task_id=task.ID,
                            worker_id=self.worker_id
                        )
                        
                        # TODO: Replace this simulation with actual task processing
                        # This would integrate with the connector and classification systems
                        time.sleep(2)  # Reduced from 5 seconds for better responsiveness
                        
                        # Report successful completion
                        self.parent.report_task_result(
                            task.ID, 
                            "COMPLETED", 
                            False,
                            {"simulation": True},
                            {"worker_id": self.worker_id}
                        )
                        
                    except queue.Empty:
                        continue  # Check shutdown event
                    except Exception as task_error:
                        # Report task failure
                        if 'task' in locals():
                            self.parent.report_task_result(
                                task.ID,
                                "FAILED",
                                True,  # Retryable
                                {"error": str(task_error)},
                                {"worker_id": self.worker_id}
                            )
                        
                        error = self.parent.error_handler.handle_error(
                            task_error,
                            "in_process_worker_task_execution",
                            operation="task_processing",
                            worker_id=self.worker_id
                        )
                        self.logger.error("Worker task execution failed", error_id=error.error_id)
                        
            except Exception as worker_error:
                error = self.parent.error_handler.handle_error(
                    worker_error,
                    "in_process_worker_main_loop",
                    operation="worker_main_loop",
                    worker_id=self.worker_id
                )
                self.logger.error("Worker thread crashed", error_id=error.error_id)
            finally:
                self.logger.log_component_lifecycle(self.worker_id, "STOPPED")

    class _CliCommandProcessor(threading.Thread):
        """Non-blocking CLI command processor."""
        
        def __init__(self, parent: 'Orchestrator'):
            super().__init__(name="CliThread", daemon=True)
            self.parent = parent

        def run(self):
            """Main CLI loop with non-blocking input."""
            time.sleep(2)  # Wait for other components to start
            
            print("\n" + "="*50)
            print("Orchestrator CLI is active.")
            print("Commands: pause <id>, resume <id>, cancel <id>, stats <id>, health, help")
            print("="*50)
            
            try:
                while not self.parent._shutdown_event.is_set():
                    try:
                        # Use select for non-blocking input on Unix-like systems
                        if HAS_SELECT:
                            ready, _, _ = select.select([sys.stdin], [], [], 1.0)
                            if ready:
                                command = sys.stdin.readline().strip()
                            else:
                                continue  # Check shutdown event
                        else:
                            # Fallback for Windows or systems without select
                            # Use a shorter prompt cycle to check shutdown more frequently
                            print("> ", end="", flush=True)
                            
                            # Simple timeout simulation by checking shutdown every iteration
                            if self.parent._shutdown_event.is_set():
                                break
                                
                            try:
                                # This will still block, but we check shutdown more frequently
                                command = input()
                            except EOFError:
                                break
                        
                        if not command:
                            continue
                            
                        self._process_command(command)
                        
                    except (EOFError, KeyboardInterrupt):
                        print("\nCLI shutting down...")
                        break
                    except Exception as e:
                        print(f"CLI Error: {e}")
                        
            except Exception as cli_error:
                error = self.parent.error_handler.handle_error(
                    cli_error,
                    "cli_command_processor",
                    operation="cli_main_loop"
                )
                print(f"CLI crashed with error: {error.error_id}")

        def _process_command(self, command: str):
            """Process a single CLI command."""
            parts = command.strip().split()
            if not parts:
                return
                
            cmd = parts[0].lower()
            args = parts[1:]

            try:
                if cmd == "pause" and args:
                    job_id = int(args[0])
                    success = self.parent.pause_job(job_id)
                    print(f"{'SUCCESS' if success else 'FAILED'}: Pause job {job_id}")
                    
                elif cmd == "resume" and args:
                    job_id = int(args[0])
                    success = self.parent.resume_job(job_id)
                    print(f"{'SUCCESS' if success else 'FAILED'}: Resume job {job_id}")
                    
                elif cmd == "cancel" and args:
                    job_id = int(args[0])
                    success = self.parent.cancel_job(job_id)
                    print(f"{'SUCCESS' if success else 'FAILED'}: Cancel job {job_id}")
                    
                elif cmd == "stats" and args:
                    job_id = int(args[0])
                    stats = self.parent.get_job_statistics(job_id)
                    print(f"Job {job_id} Statistics:")
                    for key, value in stats.items():
                        print(f"  {key}: {value}")
                        
                elif cmd == "health":
                    health = self.parent.get_health()
                    print("System Health:")
                    for key, value in health.items():
                        print(f"  {key}: {value}")
                        
                elif cmd == "help":
                    print("Available commands:")
                    print("  pause <job_id>   - Pause a running job")
                    print("  resume <job_id>  - Resume a paused job")  
                    print("  cancel <job_id>  - Cancel a job")
                    print("  stats <job_id>   - Show job statistics")
                    print("  health           - Show system health")
                    print("  help             - Show this help message")
                    
                else:
                    print(f"Unknown command: {cmd}. Type 'help' for available commands.")
                    
            except ValueError:
                print("ERROR: Invalid job ID. Please provide an integer.")
            except Exception as e:
                print(f"Command error: {e}")