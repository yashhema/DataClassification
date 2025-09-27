import asyncio
from math import ceil
import base64
import traceback
from typing import List, Dict, Any
import json

# Core system and model imports
from sqlalchemy import update
from core.db_models.job_schema import TaskOutputRecord, TaskStatus
from core.db_models.remediation_ledger_schema import LedgerStatus
from core.models.models import (
    PolicySelectorExecutePayload, PolicyActionExecutePayload, PolicyReconcilePayload,
    QueryDefinition, Pagination, ActionDefinition, TaskType, ClassificationPayload,
    DiscoveryGetDetailsPayload, DiscoveredObject, WorkPacket, WorkPacketHeader, TaskConfig,
    PolicyCommitPlanPayload
)
from core.utils.hash_utils import generate_task_id
from core.errors import ErrorCategory
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
                    
                    # Prepare lists to accumulate work for the batch transaction
                    tasks_to_create: List[Dict[str, Any]] = []
                    processed_records: List[TaskOutputRecord] = []
                    failed_record_ids: List[int] = []

                    for record in records:
                        try:
                            # This method now returns a list of dictionaries for new tasks
                            task_params_list = await self._process_output_record(record)
                            if task_params_list:
                                tasks_to_create.extend(task_params_list)
                            
                            # If processing succeeds, add the record to be marked as "PROCESSED"
                            processed_records.append(record)
                        except Exception as e:
                            # Isolate failure for a single record. Log it and add it to the failed list.
                            full_traceback = traceback.format_exc()
                            self.logger.error(
                            f"Error processing output record {record.id}: {e}",
                            record_id=record.id,
                            error_message=str(e),
                            traceback=full_traceback,
                            exc_info=True  # This tells the logger to include exception info
                            )
                            failed_record_ids.append(record.id)
                            continue
                    
                    # --- Transactional Batch Update ---
                    if processed_records or failed_record_ids:
                        self.logger.info(f"Preparing to commit batch transaction: {len(tasks_to_create)} new tasks, "
                                       f"{len(processed_records)} records to process, {len(failed_record_ids)} to fail.")
                        
                        # Start a single session for all database operations in this batch
                        async with self.db.get_async_session() as session:
                            try:
                                # 1. Create all new tasks in a single bulk operation
                                if tasks_to_create:
                                    await self.db.create_task_batch(tasks_to_create, session=session)

                                # 2. Mark all successfully processed records as "PROCESSED"
                                if processed_records:
                                    processed_ids = [r.id for r in processed_records]
                                    await self.db.update_output_record_status_batch(processed_ids, "PROCESSED", session=session)
                                
                                # 3. Mark all records that failed during processing as "FAILED"
                                if failed_record_ids:
                                    await self.db.update_output_record_status_batch(failed_record_ids, "FAILED", session=session)
                                
                                await session.commit()
                                self.logger.info("Pipeliner batch transaction committed successfully.")
                            except Exception as batch_error:
                                await session.rollback()
                                self.logger.error("Pipeliner batch transaction failed and was rolled back.", error=str(batch_error), exc_info=True)
                                # Records that failed to commit will be retried on the next cycle

                    self.orchestrator.update_thread_liveness("pipeliner")
                    await asyncio.sleep(self.interval)
                
                except Exception as e:
                    self.logger.error("An unexpected error occurred in the Pipeliner loop.", exc_info=True)
                    error = self.orchestrator.error_handler.handle_error(e, "Pipeliner_main_loop")
                    if error.error_category == ErrorCategory.FATAL_BUG:
                        self.logger.critical(f"Fatal error detected in {self.__class__.__name__}: {error}")
                        raise
                    
                    self.logger.warning(f"Transient error in {self.__class__.__name__}, retrying: {error}")
                    await asyncio.sleep(self.interval)

            self.logger.log_component_lifecycle("Pipeliner", "STOPPED")

    async def _process_output_record_not_to_use(self, record) -> List[Dict[str, Any]]:
        """
        Main dispatcher that routes records to a handler and returns the
        list of new tasks to be created.
        """
        parent_task = await self.db.get_task_by_id(record.task_id)
        
        if not parent_task or parent_task.status != TaskStatus.COMPLETED:
            self.logger.warning(f"Orphaning output record {record.id} from a non-completed or missing parent task.")
            await self.db.update_output_record_status(record.id, "ORPHANED")
            return []

        output_type = record.output_type
        if output_type == "SELECTION_PLAN_CREATED":
            return await self._fan_out_selection_tasks(parent_task, record)
        elif output_type == "ACTION_PLAN_CREATED":
            return await self._fan_out_action_tasks(parent_task, record)
        elif output_type == "METADATA_RECONCILE_UPDATES":
            return await self._create_reconciliation_task(parent_task, record)
        elif output_type in ["DISCOVERED_OBJECTS", "OBJECT_DETAILS_FETCHED"]:
            return await self._create_next_stage_scan_task(parent_task, record)
        
        return []


    async def _process_output_record(self, record) -> List[Dict[str, Any]]:
            """
            Main dispatcher that routes records to a handler and returns the
            list of new tasks to be created. The initial check is performed in a
            single transaction to prevent detached instance errors.
            """
            # Start a single session that will be used for this entire operation
            async with self.db.get_async_session() as session:
                try:
                    # 1. Fetch the parent task using the active session
                    parent_task = await self.db.get_task_by_id(record.task_id, session=session)
                    
                    # 2. Perform validation while the parent_task is still "attached"
                    if not parent_task or parent_task.status != TaskStatus.COMPLETED:
                        self.logger.warning(f"Orphaning output record {record.id} from a non-completed or missing parent task.")
                        # Use the same session to update the status
                        await self.db.update_output_record_status(record.id, "ORPHANED", session=session)
                        await session.commit() # Commit this specific action
                        return []

                    # 3. Dispatch to the correct handler to get the parameters for the next tasks.
                    #    These handlers receive the "attached" parent_task object.
                    output_type = record.output_type
                    if output_type == "SELECTION_PLAN_CREATED":
                        return await self._fan_out_selection_tasks(parent_task, record)
                    elif output_type == "ACTION_PLAN_CREATED":
                        return await self._fan_out_action_tasks(parent_task, record)
                    elif output_type == "METADATA_RECONCILE_UPDATES":
                        return await self._create_reconciliation_task(parent_task, record)
                    elif output_type in ["DISCOVERED_OBJECTS", "OBJECT_DETAILS_FETCHED"]:
                        return await self._create_next_stage_scan_task(parent_task, record)
                    
                    return []
                    
                except Exception as e:
                    # If any error occurs during this process, roll back the transaction
                    await session.rollback()
                    # Re-raise the exception to be caught by the main run_async loop's error handler
                    raise

    async def _fan_out_selection_tasks(self, parent_task, record) -> List[Dict[str, Any]]:
        """Prepares a list of POLICY_SELECTOR_EXECUTE task parameters."""
        parent_task_id_bytes = parent_task.id if isinstance(parent_task.id, bytes) else bytes.fromhex(parent_task.id)
        parent_task_id_str = parent_task.id.hex() if isinstance(parent_task.id, bytes) else parent_task.id
        
        blueprint = record.output_payload
        total_objects = blueprint.get("total_objects", 0)
        batch_size = blueprint.get("selection_batch_size", 10000)
        plan_id = blueprint.get("plan_id")
        tasks_to_create = []

        if total_objects == 0:
            self.logger.info(f"Selection plan for job {parent_task.job_id} found no objects. Proceeding to finalize.", job_id=parent_task.job_id)
            # If no objects, we can move directly to finalizing the job.
            commit_task_id_bytes = generate_task_id()
            commit_header = WorkPacketHeader(
                task_id=commit_task_id_bytes.hex(), 
                job_id=parent_task.job_id, 
                parent_task_id=parent_task_id_str
            )
            commit_payload = PolicyCommitPlanPayload(
                plan_id=plan_id, 
                action_definition=blueprint.get("action_definition")
            )
            commit_packet = WorkPacket(header=commit_header, config=TaskConfig(), payload=commit_payload)
            tasks_to_create.append({
                "job_id": parent_task.job_id, 
                "task_id": commit_task_id_bytes,
                "task_type": TaskType.POLICY_COMMIT_PLAN, 
                "work_packet": commit_packet.model_dump(mode='json'),
                "parent_task_id": parent_task_id_bytes
            })
            return tasks_to_create

        num_tasks = ceil(total_objects / batch_size)
        self.logger.info(f"Pipeliner fanning out selection phase for job {parent_task.job_id}: creating {num_tasks} tasks.", job_id=parent_task.job_id)

        query_def = QueryDefinition(**blueprint.get("query_definition", {}))

        for i in range(num_tasks):
            task_id_bytes = generate_task_id()
            header = WorkPacketHeader(
                task_id=task_id_bytes.hex(), 
                job_id=parent_task.job_id, 
                parent_task_id=parent_task_id_str
            )
            payload = PolicySelectorExecutePayload(
                plan_id=plan_id,
                query=query_def,
                pagination=Pagination(offset=(i * batch_size), limit=batch_size)
            )
            work_packet = WorkPacket(header=header, config=TaskConfig(), payload=payload)
            tasks_to_create.append({
                "job_id": parent_task.job_id, 
                "task_id": task_id_bytes,
                "task_type": TaskType.POLICY_SELECTOR_EXECUTE, 
                "work_packet": work_packet.model_dump(mode='json'),
                "parent_task_id": parent_task_id_bytes
            })
        return tasks_to_create

    async def _fan_out_action_tasks(self, parent_task, record) -> List[Dict[str, Any]]:
        """Prepares a list of POLICY_ACTION_EXECUTE task parameters."""
        parent_task_id_bytes = parent_task.id if isinstance(parent_task.id, bytes) else bytes.fromhex(parent_task.id)
        parent_task_id_str = parent_task.id.hex() if isinstance(parent_task.id, bytes) else parent_task.id
        
        action_plan = record.output_payload
        plan_id = action_plan.get("plan_id")
        action_def = ActionDefinition(**action_plan.get("action_definition"))
        tasks_to_create = []

        bins_to_process = await self.db.get_ledger_bins_by_status(plan_id, LedgerStatus.PLANNED)
        
        if not bins_to_process:
            self.logger.warning(f"Action phase for job {parent_task.job_id} triggered, but no PLANNED bins found.", job_id=parent_task.job_id)
            return []

        self.logger.info(f"Pipeliner fanning out action phase for job {parent_task.job_id}: creating {len(bins_to_process)} tasks.", job_id=parent_task.job_id)

        for ledger_bin in bins_to_process:
            task_id_bytes = generate_task_id()
            header = WorkPacketHeader(
                task_id=task_id_bytes.hex(), 
                job_id=parent_task.job_id, 
                parent_task_id=parent_task_id_str
            )
            payload = PolicyActionExecutePayload(
                plan_id=plan_id, 
                bin_id=ledger_bin.bin_id, 
                action=action_def,
                objects_to_process=[{"ObjectID": oid, "ObjectPath": opath} for oid, opath in zip(ledger_bin.object_ids, ledger_bin.object_paths)]
            )
            work_packet = WorkPacket(header=header, config=TaskConfig(), payload=payload)
            tasks_to_create.append({
                "job_id": parent_task.job_id, 
                "task_id": task_id_bytes,
                "task_type": TaskType.POLICY_ACTION_EXECUTE, 
                "work_packet": work_packet.model_dump(mode='json'),
                "parent_task_id": parent_task_id_bytes
            })
        return tasks_to_create

    async def _create_reconciliation_task(self, parent_task, record) -> List[Dict[str, Any]]:
        """Prepares the parameters for a single POLICY_RECONCILE task."""
        parent_task_id_bytes = parent_task.id if isinstance(parent_task.id, bytes) else bytes.fromhex(parent_task.id)
        parent_task_id_str = parent_task.id.hex() if isinstance(parent_task.id, bytes) else parent_task.id
        
        task_id_bytes = generate_task_id()
        header = WorkPacketHeader(
            task_id=task_id_bytes.hex(), 
            job_id=parent_task.job_id, 
            parent_task_id=parent_task_id_str
        )
        payload_data = record.output_payload
        payload = PolicyReconcilePayload(
            plan_id=parent_task.work_packet.get("payload", {}).get("plan_id"),
            updates=payload_data.get("updates", [])
        )
        work_packet = WorkPacket(header=header, config=TaskConfig(), payload=payload)
        
        return [{
            "job_id": parent_task.job_id, 
            "task_id": task_id_bytes,
            "task_type": TaskType.POLICY_RECONCILE, 
            "work_packet": work_packet.model_dump(mode='json'),
            "parent_task_id": parent_task_id_bytes
        }]

    async def _create_next_stage_scan_task(self, parent_task, record) -> List[Dict[str, Any]]:
        """
        Creates the next stage tasks in parallel by using the complete DiscoveredObject
        models provided directly in the output record from the enumeration task.
        """
        parent_task_id_bytes = parent_task.id if isinstance(parent_task.id, bytes) else bytes.fromhex(parent_task.id)
        parent_task_id_str = parent_task.id.hex() if isinstance(parent_task.id, bytes) else parent_task.id
        context = {"parent_task_id": parent_task_id_str, "record_id": record.id}
        
        job = (await self.db.get_jobs_by_ids([parent_task.job_id], context=context))[0]
        if not job:
            self.logger.error(f"Cannot pipeline task for missing job {parent_task.job_id}", **context)
            return []
        # --- DEBUGGING: DUMP JOB VARIABLE ---
        print("\n" + "="*20 + " DEBUG: DUMPING JOB OBJECT " + "="*20)
        print(f"Type of job object: {type(job)}")
        # The vars() function gives you the object's __dict__, showing its attributes
        print("Job object attributes (using vars()):")
        print(vars(job))
        print("="*60 + "\n")
        # --- END DEBUGGING ---
        output_payload = record.output_payload
        discovered_object_dicts = output_payload.get("discovered_objects", [])
        if not discovered_object_dicts:
            return []

        # --- FIX: Decode the Base64 hash string back to bytes before creating the model ---
        for data_dict in discovered_object_dicts:
            if 'object_key_hash' in data_dict and isinstance(data_dict['object_key_hash'], str):
                try:
                    data_dict['object_key_hash'] = base64.b64decode(data_dict['object_key_hash'])
                except (ValueError, TypeError) as e:
                    self.logger.error(f"Failed to decode object_key_hash for record {record.id}", error=str(e), **context)
                    # Skip this corrupted object data
                    continue
        # --- END OF FIX ---

        # 1. Re-instantiate the Pydantic models from the dictionaries in the payload
        # FIX: Only include objects that have valid object_key_hash after decoding
        discovered_objects = [DiscoveredObject(**data) for data in discovered_object_dicts if 'object_key_hash' in data]

        # 2. Get the workflow type from the job's configuration
        workflow = job.configuration.get("discovery_workflow", "single-phase")
        
        tasks_to_create = []

        # 3. Create the CLASSIFICATION task
        classification_task_id_bytes = generate_task_id()
        classification_header = WorkPacketHeader(
            task_id=classification_task_id_bytes.hex(),
            job_id=parent_task.job_id,
            parent_task_id=parent_task_id_str
        )
        # FIX: Safe datasource_id extraction with validation
        datasource_targets = job.configuration.get('datasource_targets', [])
        if not datasource_targets:
            self.logger.error(f"No datasource_targets found in job configuration for job {job.id}", **context)
            return []
        datasource_id = datasource_targets[0].get('datasource_id')
        if not datasource_id:
            self.logger.error(f"No datasource_id found in datasource_targets for job {job.id}", **context)
            return []
            
        classification_payload = ClassificationPayload(
            datasource_id=datasource_id,
            classifier_template_id=job.configuration.get("classifier_template_id"),
            discovered_objects=discovered_objects
        )
        classification_packet = WorkPacket(header=classification_header, config=TaskConfig(), payload=classification_payload)
        tasks_to_create.append({
            "job_id": parent_task.job_id, 
            "task_id": classification_task_id_bytes,
            "task_type": TaskType.CLASSIFICATION, 
            "work_packet": classification_packet.model_dump(mode='json'),
            "parent_task_id": parent_task_id_bytes
        })

        # 4. Create the GET_DETAILS task ONLY for two-phase workflows
        if workflow == "two-phase":
            details_task_id_bytes = generate_task_id()
            details_header = WorkPacketHeader(
                task_id=details_task_id_bytes.hex(),
                job_id=parent_task.job_id,
                parent_task_id=parent_task_id_str
            )
            # FIX: Use the same safely extracted datasource_id
            details_payload = DiscoveryGetDetailsPayload(
                datasource_id=datasource_id,
                discovered_objects=discovered_objects
            )
            details_packet = WorkPacket(header=details_header, config=TaskConfig(), payload=details_payload)
            tasks_to_create.append({
                "job_id": parent_task.job_id, 
                "task_id": details_task_id_bytes,
                "task_type": TaskType.DISCOVERY_GET_DETAILS, 
                "work_packet": details_packet.model_dump(mode='json'),
                "parent_task_id": parent_task_id_bytes
            })

        if tasks_to_create:
            self.logger.info(f"Pipeliner creating {len(tasks_to_create)} next-stage tasks in parallel for job {job.id}", **context)

        return tasks_to_create