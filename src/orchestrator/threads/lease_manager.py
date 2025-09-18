# src/orchestrator/threads/lease_manager.py
"""
The LeaseManager coroutine is a background process that periodically renews
the lease for all jobs currently owned by this Orchestrator instance.
It uses an optimistic locking pattern to prevent race conditions.
"""

import asyncio
from datetime import datetime, timezone, timedelta

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class LeaseManager:
    """Coroutine responsible for renewing job leases."""

    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        # We'll set the interval to be 1/3 of the lease duration for safety
        self.lease_duration_sec = 300 # e.g., 5 minutes
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
                self.orchestrator.error_handler.handle_error(e, "lease_manager_main_loop")
                # Avoid a tight loop on repeated database failures
                await asyncio.sleep(self.interval_sec)

        self.logger.log_component_lifecycle("LeaseManager", "STOPPED")

    async def _renew_all_active_leases(self):
        """Iterates through all owned jobs and attempts to renew their lease."""
        
        # Create a copy of the job IDs to prevent issues if the dictionary is modified during iteration
        async with self.orchestrator._state_lock:
            owned_jobs = list(self.orchestrator.job_states.items())

        if not owned_jobs:
            return

        self.logger.info(f"LeaseManager renewing leases for {len(owned_jobs)} active jobs.", count=len(owned_jobs))

        for job_id, state_info in owned_jobs:
            try:
                current_version = state_info.get("version")
                if current_version is None:
                    self.logger.warning(f"Cannot renew lease for job {job_id}: version number is missing from in-memory state.", job_id=job_id)
                    continue

                was_renewed = await self.db.renew_job_lease(
                    job_id=job_id,
                    orchestrator_id=self.orchestrator.instance_id,
                    current_version=current_version,
                    lease_duration_sec=self.lease_duration_sec
                )

                if was_renewed:
                    # Update the in-memory state with the new lease expiry and version
                    new_expiry = datetime.now(timezone.utc) + timedelta(seconds=self.lease_duration_sec)
                    await self.orchestrator._update_job_in_memory_state(
                        job_id=job_id,
                        version=current_version + 1,
                        lease_expiry=new_expiry
                    )
                else:
                    # If the renewal failed, we have lost the lease.
                    await self._handle_lost_lease(job_id)

            except Exception as e:
                self.orchestrator.error_handler.handle_error(e, "renew_lease_for_job", job_id=job_id)
                continue # Continue to the next job even if one fails

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
            # Safely remove the job from all in-memory structures
            self.orchestrator.job_states.pop(job_id, None)
            self.orchestrator.job_cache.pop(job_id, None)
            self.orchestrator.snoozed_jobs.pop(job_id, None)