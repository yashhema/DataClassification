import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class HealthCheck:
    """
    A supervised background coroutine that periodically checks the health of
    critical external dependencies (e.g., database connection). If a check
    persistently fails, it triggers a system-wide emergency shutdown.
    To Add granularity - not health check lead to shutdown 
    Add more tests later
    """

    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        # In a production system, this interval should be configurable.
        self.interval_seconds = 60 

    async def run_async(self)-> None:
        """The main async loop for the Health Check coroutine."""
        self.logger.log_component_lifecycle("HealthCheck", "STARTED")
        
        # Add a small initial delay to allow other components to start up
        await asyncio.sleep(10)

        while not self.orchestrator._shutdown_event.is_set():
            try:
                # --- 1. Check Database Connectivity ---
                # This is the most critical dependency for the Orchestrator.
                if not await self.db.test_connection():
                    # This is a fatal condition. If we can't talk to the DB, 
                    # the orchestrator is non-functional.
                    reason = "Database health check failed. The connection is not available."
                    await self.orchestrator.emergency_shutdown(reason)
                    return # Exit the loop immediately after initiating shutdown

                # --- 2. Check Resource Coordinator State Manager Health (e.g., Redis) ---
                # In EKS mode, this would check the Redis connection.
                # if hasattr(self.orchestrator.resource_coordinator.state_manager, 'is_healthy'):
                #     if not await self.orchestrator.resource_coordinator.state_manager.is_healthy():
                #         reason = "Resource Coordinator state manager (e.g., Redis) is not healthy."
                #         await self.orchestrator.emergency_shutdown(reason)
                #         return

                # --- 3. If all checks pass, log liveness and sleep ---
                # The TaskManager can monitor this for hangs. In a real system,
                # this would likely update a shared liveness probe state.
                self.logger.debug("Health checks passed successfully.")

            except Exception as e:
                # This catch block handles unexpected errors within the health check logic itself.
                # Such a failure is also considered fatal, as it means the system
                # can no longer monitor its own health.
                self.orchestrator.error_handler.handle_error(e, "health_check_loop_error")
                reason = f"The HealthCheck coroutine encountered an unhandled exception: {e}"
                await self.orchestrator.emergency_shutdown(reason)
                return # Exit the loop

            await asyncio.sleep(self.interval_seconds)

        self.logger.log_component_lifecycle("HealthCheck", "STOPPED")
