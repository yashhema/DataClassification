# src/orchestrator/threads/pipeliner.py
"""
The Pipeliner coroutine is a background process that enables asynchronous,
multi-stage workflows. It queries for `TaskOutputRecord`s created by workers
and generates the next set of tasks in the job's processing pipeline.

UPDATED: This version contains the comprehensive fan-out/fan-in logic
required to orchestrate the multi-stage Policy Job workflow.
"""

import asyncio
from math import ceil

# Core system and model imports
from core.db_models.job_schema import TaskStatus
from core.db_models.remediation_ledger_schema import LedgerStatus
from core.models.models import (
    PolicySelectorExecutePayload,
    PolicyActionExecutePayload,
    PolicyReconcilePayload,
    QueryDefinition,
    Pagination,
    ActionDefinition,
    TaskType
    
)

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

class Pipeliner:
    """Coroutine responsible for creating new tasks based on worker progress."""
    
    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.interval = orchestrator.settings.orchestrator.pipeliner_interval_sec
        self.name = "PipelinerCoroutine"

    async def run_async(self):
        """The main async loop for the Pipeliner coroutine."""
        self.logger.log_component_lifecycle("Pipeliner", "STARTED")
        
        while not self.orchestrator._shutdown_event.is_set():
            try:
                records = await self.db.get_pending_output_records(limit=100)
                if not records:
                    await asyncio.sleep(self.interval)
                    continue
                
                self.logger.info(f"Pipeliner found {len(records)} output records to process.", count=len(records))
                
                for record in records:
                    try:
                        await self._process_output_record(record)
                    except Exception as record_error:
                        self.logger.error(
                            f"Error processing output record {record.ID}",
                            record_id=record.ID,
                            exc_info=True
                        )
                        await self.db.update_output_record_status(record.ID, "FAILED")
                        continue

                self.orchestrator.update_thread_liveness("pipeliner")
                await asyncio.sleep(self.interval)
            
            except Exception as e:
                self.logger.error("An unexpected error occurred in the Pipeliner loop.", exc_info=True)
                await asyncio.sleep(self.interval)

        self.logger.log_component_lifecycle("Pipeliner", "STOPPED")

    async def _process_output_record(self, record):
        """
        Main dispatcher for all multi-stage workflows. It routes records to the
        correct handler based on their OutputType.
        """
        parent_task = await self.db.get_task_by_id(record.TaskID)
        
        if not parent_task or parent_task.Status != TaskStatus.COMPLETED:
            self.logger.warning(f"Orphaning output record {record.ID} from a non-completed or missing parent task.")
            await self.db.update_output_record_status(record.ID, "ORPHANED")
            return

        # --- Main Dispatcher Logic ---
        output_type = record.OutputType
        if output_type == "SELECTION_PLAN_CREATED":
            await self._fan_out_selection_tasks(parent_task, record)
        elif output_type == "ACTION_PLAN_CREATED":
            await self._fan_out_action_tasks(parent_task, record)
        elif output_type == "METADATA_RECONCILE_UPDATES":
            await self._create_reconciliation_task(parent_task, record)
        elif output_type in ["DISCOVERED_OBJECTS", "OBJECT_DETAILS_FETCHED"]:
            await self._create_next_stage_scan_task(parent_task, record)
        
        # Mark the record as processed to prevent duplicate task creation
        await self.db.update_output_record_status(record.ID, "PROCESSED")

    async def _fan_out_selection_tasks(self, parent_task, record):
        """Reads a selection plan blueprint and creates parallel POLICY_SELECTOR_EXECUTE tasks."""
        blueprint = record.OutputPayload
        total_objects = blueprint.get("total_objects", 0)
        batch_size = blueprint.get("selection_batch_size", 10000)
        plan_id = blueprint.get("plan_id")
        
        if total_objects == 0:
            self.logger.info(f"Selection plan for job {parent_task.JobID} found no objects. Proceeding to finalize.", job_id=parent_task.JobID)
            # If no objects, we can move directly to finalizing the job.
            # A simple way is to create the commit task which will find no bins and complete.
            await self.db.create_task(
                job_id=parent_task.JobID,
                task_type=TaskType.POLICY_COMMIT_PLAN,
                work_packet={"payload": {"plan_id": plan_id, "action_definition": blueprint.get("action_definition")}}
            )
            return

        num_tasks = ceil(total_objects / batch_size)
        self.logger.info(f"Pipeliner fanning out selection phase for job {parent_task.JobID}: creating {num_tasks} tasks.", job_id=parent_task.JobID)

        query_def = QueryDefinition(**blueprint.get("query_definition", {}))

        for i in range(num_tasks):
            payload = PolicySelectorExecutePayload(
                plan_id=plan_id,
                query=query_def,
                pagination=Pagination(offset=(i * batch_size), limit=batch_size)
            )
            await self.db.create_task(
                job_id=parent_task.JobID,
                task_type=TaskType.POLICY_SELECTOR_EXECUTE,
                work_packet={"payload": payload.dict()},
                parent_task_id=parent_task.ID
            )

    async def _fan_out_action_tasks(self, parent_task, record):
        """Reads an action plan and creates parallel POLICY_ACTION_EXECUTE tasks for each bin in the ledger."""
        action_plan = record.OutputPayload
        plan_id = action_plan.get("plan_id")
        action_def = ActionDefinition(**action_plan.get("action_definition"))

        bins_to_process = await self.db.get_ledger_bins_by_status(plan_id, LedgerStatus.PLANNED)
        
        if not bins_to_process:
            self.logger.warning(f"Action phase for job {parent_task.JobID} triggered, but no PLANNED bins found.", job_id=parent_task.JobID)
            return

        self.logger.info(f"Pipeliner fanning out action phase for job {parent_task.JobID}: creating {len(bins_to_process)} tasks.", job_id=parent_task.JobID)

        for ledger_bin in bins_to_process:
            payload = PolicyActionExecutePayload(
                plan_id=plan_id,
                bin_id=ledger_bin.bin_id,
                action=action_def,
                objects_to_process=[{"ObjectID": oid, "ObjectPath": opath} for oid, opath in zip(ledger_bin.ObjectIDs, ledger_bin.ObjectPaths)]
            )
            await self.db.create_task(
                job_id=parent_task.JobID,
                task_type=TaskType.POLICY_ACTION_EXECUTE,
                work_packet={"payload": payload.dict()},
                parent_task_id=parent_task.ID
            )

    async def _create_reconciliation_task(self, parent_task, record):
        """Creates a single, lightweight POLICY_RECONCILE task."""
        payload_data = record.OutputPayload
        payload = PolicyReconcilePayload(
            plan_id=parent_task.WorkPacket.get("payload", {}).get("plan_id"),
            updates=payload_data.get("updates", [])
        )
        await self.db.create_task(
            job_id=parent_task.JobID,
            task_type=TaskType.POLICY_RECONCILE,
            work_packet={"payload": payload.dict()},
            parent_task_id=parent_task.ID
        )

    async def _create_next_stage_scan_task(self, parent_task, record):
        """Original logic for standard SCANNING jobs."""
        next_task_type_str = None
        if record.OutputType == "DISCOVERED_OBJECTS":
            next_task_type_str = "DISCOVERY_GET_DETAILS"
        elif record.OutputType == "OBJECT_DETAILS_FETCHED":
            next_task_type_str = "CLASSIFICATION"

        if next_task_type_str:
            await self.db.create_task(
                job_id=parent_task.JobID,
                task_type=TaskType(next_task_type_str),
                work_packet={"payload": record.OutputPayload},
                datasource_id=parent_task.DatasourceID,
                parent_task_id=parent_task.ID
            )