# src/orchestrator/orchestrator_api_shim.py
import asyncio
import queue
from typing import TYPE_CHECKING, Dict, Any
if TYPE_CHECKING:
    from .orchestrator import Orchestrator

class OrchestratorApiShim:
    """
    A local, in-process shim that simulates a real API for the CLI.
    """
    # CORRECTED: The __init__ method now accepts the command_queue
    def __init__(self, orchestrator: "Orchestrator", command_queue: queue.Queue, loop):
        self.orchestrator = orchestrator
        self.command_queue = command_queue
        self.loop = loop
    
    def start_job(self, template_id: str, job_type_str: str):
        """Puts a 'start_job' command onto the shared queue."""
        self.command_queue.put({
            "command": "start_job",
            "template_id": template_id,
            "job_type": job_type_str
        })

    def _run_in_loop(self, coro, timeout=15):
        """Helper to run a coroutine in the main event loop from a thread."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)



    def cancel_job(self, job_id: int) -> bool:
        """Requests cancellation of a job."""
        return self._run_in_loop(self.orchestrator.cancel_job(job_id))

    def pause_job(self, job_id: int) -> bool:
        """Requests to pause a job."""
        return self._run_in_loop(self.orchestrator.pause_job(job_id))

    def resume_job(self, job_id: int) -> bool:
        """Requests to resume a paused job."""
        return self._run_in_loop(self.orchestrator.resume_job(job_id))

    def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """Gets the current status and task summary for a job."""
        return self._run_in_loop(self.orchestrator.get_job_status_summary(job_id))