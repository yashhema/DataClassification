# src/orchestrator/orchestrator.py
import asyncio
import sys
from collections import deque,defaultdict
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from .task_manager import TaskManager
# Core system imports
from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.db.database_interface import DatabaseInterface
from orchestrator.resource_coordinator import ResourceCoordinator
from core.db_models.job_schema import JobStatus
from core.errors import ErrorHandler

# Import background thread components
from .threads.task_assigner import TaskAssigner
from .threads.reaper import Reaper
from .threads.pipeliner import Pipeliner
from .threads.job_monitor import JobCompletionMonitor
from .threads.job_reaper import JobReaper
from .threads.lease_manager import LeaseManager
import time
# Import the neutral state model
from .orchestrator_state import JobState
from .healthcheck import HealthCheck
from .threads.master_job_monitor import MasterJobMonitor
from queue import Empty
from .threads.summary_reconciler import SummaryReconciler
from orchestrator.orchestrator_state import map_db_status_to_memory_state
class Orchestrator:
    """Manages the lifecycle of jobs and tasks for a specific NodeGroup."""
    @property
    def _in_process_work_queue(self):
        return self.__in_process_work_queue

    @_in_process_work_queue.setter  
    def _in_process_work_queue(self, value):
        if value is None and hasattr(self, '__in_process_work_queue') and self.__in_process_work_queue is not None:
            self.logger.warning("QUEUE SET TO NONE - this shouldn't happen!")
            import traceback
            traceback.print_stack()
        self.__in_process_work_queue = value
    VALID_TRANSITIONS = {
        JobState.QUEUED: [JobState.RUNNING, JobState.CANCELLED],
        JobState.RUNNING: [JobState.PAUSING, JobState.CANCELLING, JobState.COMPLETED, JobState.FAILED],
        JobState.PAUSING: [JobState.PAUSED, JobState.CANCELLING],
        JobState.PAUSED: [JobState.RUNNING, JobState.CANCELLING],
        JobState.CANCELLING: [JobState.CANCELLED],
        JobState.CANCELLED: [],
        JobState.COMPLETED: [],
        JobState.FAILED: [],
    }


    def __init__(self, settings: SystemConfig, db: DatabaseInterface, logger: SystemLogger, error_handler: ErrorHandler):
        self.settings = settings
        self.db = db
        self.logger = logger
        self.error_handler = error_handler
        self.instance_id = self.settings.orchestrator.instance_id
        
        self.logger.info(f"Orchestrator instance starting with stable ID: {self.instance_id}")
        
        self._shutdown_event = asyncio.Event()
        
        self._state_lock = asyncio.Lock()
        self.job_states: Dict[int, Dict[str, Any]] = {}
        self.job_cache: Dict[int, deque] = defaultdict(deque)
        self.snoozed_jobs: Dict[int, float] = {}
        self.thread_liveness: Dict[str, float] = {}
        
        self.is_single_process_mode = self.settings.orchestrator.deployment_model.upper() == "SINGLE_PROCESS"
        self._in_process_work_queue: Optional[asyncio.Queue] = None
        self._background_tasks = []
        # Pass the specific config section to the TaskManager
        self.task_manager = TaskManager(self, config=self.settings.task_manager)
        # --- Internal Components ---
        self.resource_coordinator = ResourceCoordinator(settings, logger, db)
        self.task_assigner = TaskAssigner(self)
        self.reaper = Reaper(self)
        self.job_reaper = JobReaper(self)
        self.pipeliner = Pipeliner(self)
        self.job_monitor = JobCompletionMonitor(self)
        self.lease_manager = LeaseManager(self)
        # Add this line to create the health check instance
        self.health_check = HealthCheck(self) 
        # Conditionally instantiate central components
        self.master_job_monitor = None
        self.summary_reconciler = None
        if self.settings.orchestrator.is_master_monitor_enabled:
            self.master_job_monitor = MasterJobMonitor(self)
            self.summary_reconciler = SummaryReconciler(self)
            
        self.logger.log_component_lifecycle("Orchestrator", "INITIALIZED")

    async def _perform_startup_validation(self):
        """
        Performs critical validation before starting coroutines.
        For the central manager, this prevents a "split-brain" scenario.
        """
        if self.settings.orchestrator.is_master_monitor_enabled:
            self.logger.info("This instance is configured as the central manager. Performing startup validation...")
            is_lock_acquired = await self.db.set_and_verify_master_monitor_instance(self.instance_id)
            if not is_lock_acquired:
                self.logger.critical(
                    "STARTUP FAILED: Another active master monitor instance was detected. "
                    "Correct the configuration to ensure only one orchestrator has 'is_master_monitor_enabled' set to true. Exiting."
                )
                # Hard exit is the safest action to prevent a split-brain
                sys.exit(1)
            else:
                self.logger.info("Startup validation successful. This instance is the active central manager.")

    async def start_async(self, in_process_queue: Optional[asyncio.Queue] = None):
        """Starts all background coroutines for the Orchestrator service."""
        try:
            if self.is_single_process_mode:
                if not in_process_queue:
                    raise ValueError("An in-process queue is required for single_process mode.")
                self._in_process_work_queue = in_process_queue

            # Perform startup validation before starting any coroutines
            await self._perform_startup_validation()

            self.logger.log_component_lifecycle("Orchestrator", "STARTING_ASYNC_COROUTINES")
            
            await self._recover_owned_jobs_on_startup_async()

            
            
            # Conditionally add central coroutines to the startup list

            

            
            self.logger.log_component_lifecycle("Orchestrator", "ALL_ASYNC_COROUTINES_STARTED")
            self.logger.log_component_lifecycle("Orchestrator", "VALIDATING AND STARTING SUPERVISED COROUTINES")
            
            try:
                # --- Startup Failure Cascade Prevention & Graceful Degradation ---
                # First, start all essential tasks. If any fail immediately, the entire orchestrator will fail to start.
                self.logger.info("Starting essential tasks...")
                await self.task_manager.start_supervised_task("TaskAssigner", self.task_assigner.run_async, is_essential=True)
                await self.task_manager.start_supervised_task("LeaseManager", self.lease_manager.run_async, is_essential=True)
                

                # Then, start non-essential tasks. Their failure will be managed but won't stop startup.
                self.logger.info("Starting non-essential tasks...")
                await self.task_manager.start_supervised_task("Reaper", self.reaper.run_async, is_essential=False)
                await self.task_manager.start_supervised_task("JobReaper", self.job_reaper.run_async, is_essential=False)
                await self.task_manager.start_supervised_task("Pipeliner", self.pipeliner.run_async, is_essential=False)
                await self.task_manager.start_supervised_task("JobMonitor", self.job_monitor.run_async, is_essential=False)
                await self.task_manager.start_supervised_task("HealthCheck", self.health_check.run_async, is_essential=True)
                # ---  BLOCK TO CORRECTLY START CENTRAL COMPONENTS ---
                if self.master_job_monitor:
                    self.logger.info("Starting central manager task: MasterJobMonitor")
                    await self.task_manager.start_supervised_task("MasterJobMonitor", self.master_job_monitor.run_async, is_essential=False)
                if self.summary_reconciler:
                    self.logger.info("Starting central manager task: SummaryReconciler")
                    await self.task_manager.start_supervised_task("SummaryReconciler", self.summary_reconciler.run_async, is_essential=False)
                # ----------------------------------------------------------------


            except RuntimeError as e:
                self.logger.critical(f"A critical component failed its startup validation: {e}", exc_info=True)
                await self.emergency_shutdown(f"Critical component startup failure: {e}")
                return

            # The supervisor itself is the primary background task.
            supervisor_task = asyncio.create_task(self.task_manager.supervise_tasks())
            self._background_tasks.append(supervisor_task)
            
            self.logger.log_component_lifecycle("Orchestrator", "ALL_COROUTINES_STARTED")            
        except Exception as e:
            self.error_handler.handle_error(e, "orchestrator_async_startup")
            raise


    async def emergency_shutdown(self, reason: str):
        """Immediate, non-graceful shutdown with a more robust cleanup timeout."""
        if self._shutdown_event.is_set(): return

        self.logger.critical(f"EMERGENCY SHUTDOWN INITIATED. Reason: {reason}")
        self._shutdown_event.set()

        # Consolidate all managed tasks for cancellation
        all_tasks = self._background_tasks + [task for _, task, _ in self.task_manager.critical_tasks.values()]
        for task in all_tasks:
            if not task.done():
                task.cancel()

        try:
            # Wait for all tasks to acknowledge cancellation with a generous timeout
            await asyncio.wait_for(asyncio.gather(*all_tasks, return_exceptions=True), timeout=10.0)
            self.logger.info("All background tasks gracefully cancelled during emergency shutdown.")
        except asyncio.TimeoutError:
            self.logger.warning("Cleanup timeout exceeded during emergency shutdown. Forcing exit.")
        except Exception as e:
            self.logger.error(f"Error during shutdown cleanup: {e}")

        sys.exit(1)

            
    async def shutdown_async(self):
        """Initiates a graceful shutdown of the Orchestrator and its components."""
        try:
            self.logger.log_component_lifecycle("Orchestrator", "SHUTTING_DOWN_ASYNC")
            self._shutdown_event.set()
            
            if self._background_tasks:
                for task in self._background_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            
            self.logger.log_component_lifecycle("Orchestrator", "ASYNC_SHUTDOWN_COMPLETE")
        except Exception as e:
            self.error_handler.handle_error(e, "orchestrator_async_shutdown")



    async def handle_job_command_async(self, job, command: str):
            """
            Dispatches a command from the CLI to the appropriate handler, now with
            idempotent logic to prevent race conditions from stale commands.
            """
            command = command.upper()
            self.logger.info(f"Orchestrator received command '{command}' for job {job.id} in state '{job.status.value}'", job_id=job.id)
            
            # --- Idempotent Command Handling Logic ---

            if command == "PAUSE":
                # Only pause a running job. If it's already pausing, paused, or cancelled, just clear the command.
                if job.status == JobStatus.RUNNING:
                    await self.db.acknowledge_and_update_job_status(job.id, JobStatus.PAUSING)
                    self.logger.info(f"[DEBUG] handle_job_command_async:pause about to call _update_job_in_memory_state")
                    await self._update_job_in_memory_state(job.id, JobState.PAUSING)
                else:
                    self.logger.info(f"Ignoring stale PAUSE command for job {job.id}; status is '{job.status.value}'. Clearing command.", job_id=job.id)
                    await self.db.update_job_command(job.id, None)

            elif command == "CANCEL":
                # A job can be cancelled from almost any state, unless it's already terminal.
                if job.status not in [JobStatus.CANCELLED, JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.COMPLETED_WITH_FAILURES]:
                    cancelled_count = await self.db.cancel_pending_tasks_for_job(job.id)
                    self.logger.info(f"Cancelled {cancelled_count} pending tasks for job {job.id}.", job_id=job.id)
                    await self.db.acknowledge_and_update_job_status(job.id, JobStatus.CANCELLING)
                    self.logger.info(f"[DEBUG] handle_job_command_async:CANCEL about to call _update_job_in_memory_state")

                    await self._update_job_in_memory_state(job.id, JobState.CANCELLING)
                else:
                    self.logger.info(f"Ignoring stale CANCEL command for job {job.id}; status is '{job.status.value}'. Clearing command.", job_id=job.id)
                    await self.db.update_job_command(job.id, None)

            elif command == "RESUME":
                # Only resume a paused job.
                if job.status == JobStatus.PAUSED:
                    await self.db.acknowledge_and_update_job_status(job.id, JobStatus.RUNNING)
                    self.logger.info(f"[DEBUG] handle_job_command_async:RESUME about to call _update_job_in_memory_state")

                    await self._update_job_in_memory_state(job.id, JobState.RUNNING)
                else:
                    self.logger.info(f"Ignoring stale RESUME command for job {job.id}; status is '{job.status.value}'. Clearing command.", job_id=job.id)
                    await self.db.update_job_command(job.id, None)

            elif command == "START":
                # The job has been seen. The command has served its purpose.
                # We can always safely clear it, regardless of the job's current state.
                # This prevents the RUNNING -> RUNNING transition and resolves the race condition.
                self.logger.info(f"Acknowledging and clearing START command for job {job.id}.", job_id=job.id)
                await self.db.update_job_command(job.id, None)

            
            else:
                self.logger.warning(f"Unknown command '{command}' received for job {job.id}", job_id=job.id)
                # Safely clear the unknown command as well
                await self.db.update_job_command(job.id, None)


    async def _handle_pause_command_async(self, job):
        """Transitions a job to the PAUSING state."""
        if job.status not in [JobStatus.RUNNING, JobStatus.QUEUED]:
            self.logger.warning(f"Cannot pause job {job.id}, it is not in a pausable state.", job_id=job.id)
            return

        await self.db.acknowledge_and_update_job_status(job.id, JobStatus.PAUSING)
        await self._update_job_in_memory_state(job.id, JobState.PAUSING)

    async def _handle_cancel_command_async(self, job):
        """Transitions a job to the CANCELLING state and cancels its pending tasks."""
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            self.logger.warning(f"Cannot cancel job {job.id}, it is already in a terminal state.", job_id=job.id)
            return
        
        # First, cancel all tasks that haven't been dispatched yet
        cancelled_count = await self.db.cancel_pending_tasks_for_job(job.id)
        self.logger.info(f"Cancelled {cancelled_count} pending tasks for job {job.id}.", job_id=job.id)

        # Then, update the job state
        await self.db.acknowledge_and_update_job_status(job.id, JobStatus.CANCELLING)
        await self._update_job_in_memory_state(job.id, JobState.CANCELLING)

    async def _handle_resume_command_async(self, job):
        """Transitions a job from PAUSED back to RUNNING."""
        if job.status != JobStatus.PAUSED:
            self.logger.warning(f"Cannot resume job {job.id}, it is not paused.", job_id=job.id)
            return

        await self.db.acknowledge_and_update_job_status(job.id, JobStatus.RUNNING)
        await self._update_job_in_memory_state(job.id, JobState.RUNNING)


    async def get_task_async(self, worker_id: str) -> Optional[Dict[str, Any]]:
        # Change from:
        self.logger.info(f"In get_task_async")
        if self._in_process_work_queue.qsize() == 0:
            return None

        try:
            self.logger.info(f"Queue has task, get_task_async")
            task = await asyncio.wait_for(self._in_process_work_queue.get(), timeout=0.1)
            work_packet_dict = task.work_packet
            self.logger.info(f"Dequeued task {task.id} for worker {worker_id}")
            return work_packet_dict            
        except asyncio.TimeoutError:
            self.logger.info(f"Queue gave timeout error")
            return None
        except Exception as e:
            self.error_handler.handle_error(e, "get_task_async")
            return None

    async def get_task_async_NotToUseForNow(self, worker_id: str) -> Optional[Dict[str, Any]]:
        if not self.is_single_process_mode:
            return None
        
        # Check queue size first - returns immediately
        if self._in_process_work_queue.qsize() == 0:
            return None
        
        try:
            task = self._in_process_work_queue.get_nowait()
            work_packet_dict = task.work_packet
            self.logger.info(f"Dequeued task {task.id} for worker {worker_id}")
            return work_packet_dict
        except Empty:  # Handle race condition where queue became empty
            return None
        except Exception as e:
            self.error_handler.handle_error(e, "get_task_async")
            return None

    def _release_resources_for_task(self, task, context: Optional[Dict] = None):
        """Centralized method to release all resources held by a task."""
        context = context or {}
        try:
            # Release the datasource connection slot if one was used
            if task.datasource_id:
                self.resource_coordinator.release_task_reservation(task.datasource_id)

            # Release the fair-sharing worker slot for the job
            self.resource_coordinator.state_manager.release_job_worker_slot(task.job_id)
            
            self.logger.info("Resources released successfully", task_id=task.id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, "release_resources_for_task", task_id=task.id, **context)

    async def report_task_result_async(self, task_id: int, status: str, is_retryable: bool, 
                                       result_payload: Optional[Dict] = None, context: Optional[Dict] = None):
        """Reports the final result of a task within a single database transaction."""
        context = context or {}
        task = None
        try:
            # The session manager ensures the DB operations are atomic
            async with self.db.get_async_session() as session:
                task = await self.db.get_task_by_id(task_id, context=context)
                if not task:
                    self.logger.warning("Received result for an unknown or already processed task.", task_id=task_id)
                    return

                if status.upper() == "COMPLETED":
                    await self.db.complete_task(task_id, session=session, context=context)
                else:
                    await self.db.fail_task(task_id, is_retryable, self.settings.worker.max_retries, session=session, context=context)

        except Exception as e:
            self.error_handler.handle_error(e, "report_task_result_async", operation="task_db_update", task_id=task_id)
            # We still need to try and release resources even if the DB update fails
        finally:
            if task:
                self._release_resources_for_task(task, context)

    async def update_task_progress_async(self, task_id: int, progress_record: Dict, 
                                         context: Optional[Dict] = None) -> None:
        """Reports intermediate progress from a running task."""
        context = context or {}
        try:
            await self.db.write_task_output_record(
                task_id=task_id,
                output_type=progress_record.get("output_type", "UNKNOWN"),
                payload=progress_record.get("output_payload", {}),
                context=context
            )
            
            self.logger.log_progress_batch(
                progress_record.get("output_payload", {}),
                task_id,
                sampling_rate=self.settings.logging.progress_sampling_rate
            )
            
        except Exception as e:
            self.error_handler.handle_error(e, "update_task_progress_async", operation="progress_update", task_id=task_id)


    async def get_job_state_safely(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Thread-safely retrieves the in-memory state for a job.
        Returns None if the job is not in the state dictionary, preventing KeyErrors.
        """
        async with self._state_lock:
            return self.job_states.get(job_id)

    async def _garbage_collect_states_async(self):
        """Periodically cleans up terminal job states marked for deletion."""
        # This interval should be configurable in system_parameters
        interval = self.settings.orchestrator.reaper_interval_seconds * 2 
        
        while not self._shutdown_event.is_set():
            await asyncio.sleep(interval)
            
            try:
                async with self._state_lock:
                    jobs_to_delete = [
                        job_id for job_id, state in self.job_states.items()
                        if state.get("status") == JobState.TERMINATED
                    ]

                    if jobs_to_delete:
                        self.logger.info(f"Garbage collecting {len(jobs_to_delete)} terminal job states.")
                        for job_id in jobs_to_delete:
                            self.job_cache.pop(job_id, None)
                            self.snoozed_jobs.pop(job_id, None)
                            self.job_states.pop(job_id, None)
            except Exception as e:
                self.error_handler.handle_error(e, "garbage_collect_states")



    def update_thread_liveness(self, thread_name: str):
        """Allows coroutines to report that they are alive and functioning."""
        self.thread_liveness[thread_name] = time.time()

    async def is_healthy(self) -> bool:
        """Checks if the Orchestrator can communicate with the database."""
        return await self.db.test_connection()

    def _validate_state_transition(self, job_id: int, from_state: JobState, to_state: JobState) -> bool:
        """Validates if a job state transition is allowed."""
        if from_state == to_state:
            return True # A transition to the same state is always valid.

        valid_targets = self.VALID_TRANSITIONS.get(from_state, [])
        if to_state not in valid_targets:
            # DEBUGGING: Capture who is calling this
            import traceback
            call_stack = ''.join(traceback.format_stack())
            
            self.logger.warning(
                f"Invalid state transition for job {job_id}: {from_state.value} -> {to_state.value}",
                job_id=job_id
            )
            self.logger.warning(f"Call stack for invalid transition:\n{call_stack}")
            return False
        return True

    async def _update_job_in_memory_state(self, job_id: int, new_status: Optional[JobState] = None,
                                          version: Optional[int] = None,
                                          lease_expiry: Optional[datetime] = None,
                                          is_new: bool = False):
        """The single, thread-safe method for updating a job's in-memory state."""
        async with self._state_lock:
            # DEBUGGING: Log who is calling this method
            import traceback
            caller_info = traceback.format_stack()[-2].strip()
            self.logger.debug(f"[DEBUG] _update_job_in_memory_state called by: {caller_info}")
            self.logger.debug(f"[DEBUG] Parameters: job_id={job_id}, new_status={new_status}, is_new={is_new}")
            
            current_state_info = self.job_states.get(job_id, {})
            current_status = current_state_info.get("status")

            final_status = new_status or current_status
            if not final_status:
                self.logger.warning("State update called with no new or existing status.", job_id=job_id)
                return

            if not is_new and not self._validate_state_transition(job_id, current_status, final_status):
                return

            self.job_states[job_id] = {
                "status": final_status,
                "version": version or current_state_info.get("version"),
                "lease_expiry": lease_expiry or current_state_info.get("lease_expiry")
            }

            self.logger.info(
                f"Job {job_id} in-memory state changed: {current_status.value if current_status else 'NEW'} -> {final_status.value}",
                job_id=job_id
            )

            terminal_states = {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED}
            if final_status in terminal_states:
                self.job_cache.pop(job_id, None)
                self.snoozed_jobs.pop(job_id, None)
                self.job_states.pop(job_id, None)
                self.logger.info(f"Cleaned up in-memory cache for terminal job {job_id}", job_id=job_id)

    async def _recover_owned_jobs_on_startup_async(self):
        """Finds and reclaims any jobs this instance was previously managing."""
        self.logger.info(f"Checking for recoverable jobs for orchestrator ID '{self.instance_id}'...")
        try:
            recoverable_jobs = await self.db.get_recoverable_jobs_for_orchestrator(self.instance_id)
            if not recoverable_jobs:
                self.logger.info("No recoverable jobs found.")
                return

            self.logger.warning(f"Found {len(recoverable_jobs)} recoverable jobs. Reclaiming ownership...")
            for job in recoverable_jobs:
                lease_duration_sec = 300
                was_renewed = await self.db.renew_job_lease(
                    job_id=job.id,
                    orchestrator_id=self.instance_id,
                    current_version=job.version,
                    lease_duration_sec=lease_duration_sec
                )
                if was_renewed:
                    # FIXED: Actually map the job's current status
                    target_memory_state = map_db_status_to_memory_state(job.status)
                    
                    await self._update_job_in_memory_state(
                        job_id=job.id,
                        new_status=target_memory_state,  # Always provide the actual status
                        version=job.version + 1,
                        lease_expiry=datetime.now(timezone.utc) + timedelta(seconds=lease_duration_sec),
                        is_new=True  # This should bypass state transition validation
                    )
                    self.logger.info(f"Successfully reclaimed ownership of job {job.id}.", job_id=job.id)
                else:
                    self.logger.warning(f"Failed to reclaim job {job.id}. Another instance may have taken over.", job_id=job.id)
        except Exception as e:
            self.error_handler.handle_error(e, "recover_owned_jobs")