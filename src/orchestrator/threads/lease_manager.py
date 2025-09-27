# src/orchestrator/threads/lease_manager.py
"""
The LeaseManager coroutine is a background process that periodically renews
the lease for all jobs currently owned by this Orchestrator instance.
It uses an optimistic locking pattern and an atomic database operation to
process asynchronous commands sent from the CLI.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING

# NEW: Import the new safe mapping function and result type
from orchestrator.orchestrator_state import map_db_status_to_memory_state
from core.db.database_interface import LeaseRenewalResult
from core.errors import ErrorCategory
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class LeaseManager:
    """Coroutine responsible for renewing job leases and processing commands."""

    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.lease_duration_sec = 300 # 5 minutes
        self.interval_sec = self.lease_duration_sec / 3
        self.name = "LeaseManagerCoroutine"

    async def run_async(self):
        """The main async loop for the Lease Manager."""
        self.logger.log_component_lifecycle("LeaseManager", "STARTED")

        while not self.orchestrator._shutdown_event.is_set():
            try:
                await self._renew_all_active_leases()
                
                self.orchestrator.update_thread_liveness("lease_manager")
                await asyncio.sleep(self.interval_sec)

            except Exception as e:
                
                error = self.orchestrator.error_handler.handle_error(e, "lease_manager_main_loop")
                
                # Fatal error detection and propagation
                if error.error_category == ErrorCategory.FATAL_BUG:
                    self.logger.critical(f"Fatal error detected in {self.__class__.__name__}: {error}")
                    raise  # Propagate to TaskManager
                
                # Existing retry logic for operational errors
                self.logger.warning(f"Transient error in {self.__class__.__name__}, retrying: {error}")
                await asyncio.sleep(self.interval * 5)
                

        self.logger.log_component_lifecycle("LeaseManager", "STOPPED")

    async def _renew_all_active_leases(self):
        """
        Atomically renews leases and processes commands for all owned jobs.
        """
        owned_job_ids = []
        async with self.orchestrator._state_lock:
            if not self.orchestrator.job_states:
                return
            owned_job_ids = list(self.orchestrator.job_states.keys())

        if not owned_job_ids:
            return
            
        self.logger.info(f"LeaseManager checking {len(owned_job_ids)} active jobs.", count=len(owned_job_ids))

        for job_id in owned_job_ids:
            try:
                state_info = await self.orchestrator.get_job_state_safely(job_id)
                if not state_info:
                    continue 

                current_version = state_info.get("version")
                if current_version is None:
                    self.logger.warning(f"Cannot renew lease for job {job_id}: version missing from in-memory state.", job_id=job_id)
                    continue

                # A single atomic call to renew the lease AND process any command,for now we are not using version as check.
                self.logger.info(f"[DEBUG] LeaseManager about to renew lease for job {job_id}")
                result: LeaseRenewalResult = await self.db.renew_lease_and_process_command(
                    job_id=job_id,
                    orchestrator_id=self.orchestrator.instance_id,
                    current_version=current_version,
                    lease_duration_sec=self.lease_duration_sec
                )
                self.logger.info(f"[DEBUG] LeaseManager result: {result.outcome if result else 'None'}")
                # Process the structured result
                if result.outcome == "SUCCESS":
                    new_expiry = datetime.now(timezone.utc) + timedelta(seconds=self.lease_duration_sec)
                    
                    # Use the safe mapping function to convert the DB status to the in-memory state
                    new_in_memory_state = map_db_status_to_memory_state(result.new_status)
                    self.logger.info(f"[DEBUG] lease_manager about to call _update_job_in_memory_state")
                    await self.orchestrator._update_job_in_memory_state(
                        job_id=job_id,
                        new_status=new_in_memory_state,
                        version=result.new_version,
                        lease_expiry=new_expiry
                    )
                elif result.outcome == "LOCK_FAILED":
                    # The optimistic lock failed. Another process has taken ownership.
                    await self._handle_lost_lease(job_id)
                elif result.outcome == "ERROR":
                    # An unexpected DB error occurred and was handled by the ErrorHandler.
                    # We log it here but do not purge the job from memory, as it may be a transient issue.
                    self.logger.error(f"A database error occurred while trying to renew the lease for job {job_id}. Will retry on the next cycle.", job_id=job_id)

            except Exception as e:
                self.orchestrator.error_handler.handle_error(e, "renew_lease_for_job", job_id=job_id)
                continue

    async def _handle_lost_lease(self, job_id: int):
        """
        Handles the critical scenario where this orchestrator has lost the lease for a job.
        It purges all in-memory data related to the job to stop all work on it.
        """
        self.logger.warning(
            f"Lease renewal failed for job {job_id}. Another orchestrator has likely taken ownership. Purging job from memory.",
            job_id=job_id
        )
        async with self.orchestrator._state_lock:
            # Safely remove the job from all in-memory structures to halt all processing
            self.orchestrator.job_states.pop(job_id, None)
            self.orchestrator.job_cache.pop(job_id, None)
            self.orchestrator.snoozed_jobs.pop(job_id, None)

