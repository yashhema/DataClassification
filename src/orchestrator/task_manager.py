import asyncio
import time
import random
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Any, Callable, Coroutine, Tuple

from core.errors import ErrorCategory
from core.config.configuration_manager import TaskManagerConfig

if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class TaskManager:
    """
    A robust supervisor for managing the lifecycle of critical background coroutines.
    It provides fault tolerance through managed restarts with exponential backoff,
    time-based failure analysis, a circuit breaker pattern, and self-supervision.
    """

    def __init__(self, orchestrator: "Orchestrator", config: TaskManagerConfig):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.config = config
        self.critical_tasks: Dict[str, Tuple[Callable[[], Coroutine[Any, Any, None]], asyncio.Task, bool]] = {}
        self.failure_history = defaultdict(list)
        self.circuit_breakers = {}

        # Metrics for observability
        self.restart_count = defaultdict(int)
        self.lifetime_restart_count = defaultdict(int)
        self.total_failures = 0
        self.start_time = time.time()

    async def start_supervised_task(self, name: str, coro_factory: Callable[[], Coroutine[Any, Any, None]], is_essential: bool = True):
        """Starts, registers, and validates a task for supervision."""
        self.logger.info(f"Starting supervised task: {name} (Essential: {is_essential})")
        task = asyncio.create_task(coro_factory(), name=name)
        self.critical_tasks[name] = (coro_factory, task, is_essential)

        await asyncio.sleep(self.config.startup_validation_seconds)
        if task.done() and task.exception():
            raise RuntimeError(f"Essential task '{name}' failed immediately on startup: {task.exception()}")
        
        return task

    async def supervise_tasks(self):
        """The main supervisor loop. It monitors its own health and that of its child tasks."""
        self.logger.log_component_lifecycle("TaskManagerSupervisor", "STARTED")
        try:
            while not self.orchestrator._shutdown_event.is_set():
                for name, (factory, task, is_essential) in list(self.critical_tasks.items()):
                    if task.done() and not task.cancelled() and task.exception():
                        await self._handle_task_failure(name, task.exception(), is_essential)
                await asyncio.sleep(5)
        except Exception as e:
            self.logger.critical(f"FATAL: The TaskManager supervisor has crashed: {e}", exc_info=True)
            await self.orchestrator.emergency_shutdown("TaskManager supervisor failure")
        
        self.logger.log_component_lifecycle("TaskManagerSupervisor", "STOPPED")

    async def _handle_task_failure(self, name: str, exception: Exception, is_essential: bool):
        """Handles a failed task, deciding whether to restart, suspend, or shut down."""
        now = time.time()
        self.failure_history[name].append(now)
        self.total_failures += 1
        
        factory, old_task, _ = self.critical_tasks.pop(name)
        
        if not old_task.cancelled():
            old_task.cancel()
            try:
                await old_task
            except asyncio.CancelledError:
                self.logger.debug(f"Successfully cancelled failed task '{name}'.")

        error = self.orchestrator.error_handler.handle_error(exception, f"coroutine_crash_{name}")

        if error.error_category in [ErrorCategory.FATAL_BUG, ErrorCategory.CONFIGURATION]:
            reason = f"Task '{name}' failed with a non-recoverable error: {error.message}"
            await self.orchestrator.emergency_shutdown(reason)
            return

        if self._should_restart_task(name):
            self.logger.warning(f"Restarting task '{name}' after a recoverable, non-fatal error.")
            self.restart_count[name] += 1
            
            num_recent_failures = len(self.failure_history[name])
            delay = min(self.config.restart_backoff_base ** num_recent_failures, self.config.restart_backoff_cap_seconds)
            jitter = delay * random.uniform(0.5, 1.5)
            self.logger.info(f"Applying backoff delay of {jitter:.2f} seconds before restarting '{name}'.")
            await asyncio.sleep(jitter)

            await self.start_supervised_task(name, factory, is_essential)
        else:
            reason = f"Task '{name}' has failed too frequently or its circuit breaker is open. Escalating to fatal."
            if is_essential:
                await self.orchestrator.emergency_shutdown(reason)
            else:
                self.logger.critical(f"SUSPENDING non-essential task '{name}' after repeated failures.")

    def _should_restart_task(self, task_name: str) -> bool:
        """Implements time-based failure analysis and a circuit breaker to prevent crash loops."""
        now = time.time()
        
        if task_name in self.circuit_breakers:
            breaker_info = self.circuit_breakers[task_name]
            if breaker_info['state'] == 'OPEN' and now < breaker_info['open_until']:
                return False
            elif breaker_info['state'] == 'OPEN':
                self.circuit_breakers.pop(task_name)

        cutoff_time = now - 3600
        self.failure_history[task_name] = [ts for ts in self.failure_history[task_name] if ts > cutoff_time]
        recent_failures = self.failure_history[task_name]

        if len(recent_failures) > self.config.hourly_failure_threshold:
            return False

        if len(recent_failures) >= 2 and (recent_failures[-1] - recent_failures[-2] < self.config.rapid_failure_seconds):
            return False
            
        if self.restart_count[task_name] >= self.config.circuit_breaker_threshold:
            self.circuit_breakers[task_name] = {'state': 'OPEN', 'open_until': now + self.config.circuit_breaker_cooldown_seconds}
            self.lifetime_restart_count[task_name] += self.restart_count.pop(task_name, 0)
            return False
            
        return True

    def get_supervisor_metrics(self) -> Dict[str, Any]:
        """Provides key metrics for system observability."""
        return {
            "supervisor_uptime_seconds": time.time() - self.start_time,
            "total_task_failures": self.total_failures,
            "task_restart_counts_current_cycle": dict(self.restart_count),
            "task_restart_counts_lifetime": dict(self.lifetime_restart_count),
            "active_supervised_tasks": len(self.critical_tasks),
            "circuit_breaker_status": self.circuit_breakers
        }

    async def export_metrics(self):
        """Placeholder for integration with a monitoring system like Prometheus."""
        metrics = self.get_supervisor_metrics()
        self.logger.info("Exporting TaskManager metrics.", metrics=metrics)

