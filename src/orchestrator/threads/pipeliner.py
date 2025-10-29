import asyncio
from math import ceil
import base64
import traceback
from typing import List, Dict, Any,Optional
import json

# Core system and model imports
from sqlalchemy import update

from core.db_models.job_schema import Task, TaskOutputRecord, TaskStatus
from core.db_models.remediation_ledger_schema import LedgerStatus
from core.models.models import (
    PolicySelectorExecutePayload, PolicyActionExecutePayload, PolicyReconcilePayload,
    QueryDefinition, Pagination, ActionDefinition, TaskType, ClassificationPayload,
    DiscoveryGetDetailsPayload, DiscoveredObject, WorkPacket, WorkPacketHeader, TaskConfig,
    PolicyCommitPlanPayload,DiscoveryEnumeratePayload
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
                    await self.process_batch(limit=100)
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


    async def process_batch(self, job_id_filter: Optional[int] = None, limit: int = 100) -> int:
        """
        Process one batch of pending output records and create follow-up tasks.
        
        Args:
            job_id_filter: If provided, only process records for this job
            limit: Maximum records to fetch
        
        Returns:
            Number of tasks created
        """
        records = await self.db.get_pending_output_records(limit=limit)
        
        if job_id_filter:
            records = [r for r in records if r.job_id == job_id_filter]
        
        if not records:
            return 0
        
        tasks_to_create = []
        processed_record_ids = []
        failed_record_ids = []
        
        for record in records:
            try:
                task_params_list = await self._process_output_record(record)
                if task_params_list:
                    tasks_to_create.extend(task_params_list)
                processed_record_ids.append(record.id)
            except Exception as e:
                full_traceback = traceback.format_exc()
                self.logger.error(
                    f"Error processing output record {record.id}: {e}",
                    record_id=record.id,
                    error_message=str(e),
                    traceback=full_traceback,
                    exc_info=True
                )
                failed_record_ids.append(record.id)
        
        if tasks_to_create or processed_record_ids or failed_record_ids:
            async with self.db.get_async_session() as session:
                try:
                    if tasks_to_create:
                        await self.db.create_task_batch(tasks_to_create, session=session)
                    if processed_record_ids:
                        await self.db.update_output_record_status_batch(processed_record_ids, "PROCESSED", session=session)
                    if failed_record_ids:
                        await self.db.update_output_record_status_batch(failed_record_ids, "FAILED", session=session)
                    await session.commit()
                except Exception as batch_error:
                    await session.rollback()
                    self.logger.error("Pipeliner batch transaction failed and was rolled back.", error=str(batch_error), exc_info=True)
                    raise
        
        return len(tasks_to_create)


    async def _process_output_record(self, record: TaskOutputRecord) -> List[Dict[str, Any]]:
        """
        Main dispatcher that routes a record to the correct handler.
        Creates DISCOVERY_GET_DETAILS if needed.
        Handles policy workflow outputs.
        Does NOT create CLASSIFICATION tasks.
        """
        tasks_to_create = []
        parent_task = None

        async with self.db.get_async_session() as session:
            try:
                # Fetch the parent task using the active session
                parent_task = await self.db.get_task_by_id(record.task_id, session=session) # Pass session

                # Validate parent task state
                if not parent_task or parent_task.status != TaskStatus.COMPLETED:
                    self.logger.warning(f"Orphaning output record {record.id} from a non-completed or missing parent task {record.task_id.hex() if record.task_id else 'None'}.")
                    await self.db.update_output_record_status(record.id, "ORPHANED", session=session) # Pass session
                    await session.commit() # Commit this specific action
                    return [] # Exit early

                output_type = record.output_type
                context = {"job_id": parent_task.job_id, "parent_task_id": parent_task.id.hex(), "record_id": record.id}

                # --- ROUTING LOGIC ---
                if output_type == "NEW_ENUMERATION_BOUNDARIES":
                    # Handles fanning out DISCOVERY_ENUMERATE tasks for sub-boundaries
                    tasks_to_create = await self._create_new_enumeration_tasks(parent_task, record, context)
                elif output_type == "DISCOVERED_OBJECTS":
                    # *** CORRECTED LOGIC ***
                    # This output now triggers DISCOVERY_GET_DETAILS *if* required by the workflow.
                    # It NO LONGER triggers CLASSIFICATION tasks.
                    tasks_to_create = await self._create_next_stage_scan_tasks(parent_task, record, context) # Reinstated call
                elif output_type == "SELECTION_PLAN_CREATED":
                     # Handles fanning out POLICY_SELECTOR_EXECUTE or POLICY_COMMIT_PLAN tasks
                     tasks_to_create = await self._fan_out_selection_tasks(parent_task, record, context)
                elif output_type == "ACTION_PLAN_CREATED":
                     # Handles fanning out POLICY_ACTION_EXECUTE tasks
                     tasks_to_create = await self._fan_out_action_tasks(parent_task, record, context)
                elif output_type == "METADATA_RECONCILE_UPDATES":
                     # Creates the final POLICY_RECONCILE task
                     tasks_to_create = await self._create_reconciliation_task(parent_task, record, context)
                # Add other policy-related output types if needed
                else:
                    self.logger.warning(f"Unknown TaskOutputRecord type '{output_type}' for record {record.id}.", **context)
                    tasks_to_create = []

            except Exception as e:
                parent_task_id_str = parent_task.id.hex() if parent_task and parent_task.id else 'None'
                self.orchestrator.error_handler.handle_error(e, "_process_output_record_dispatch", record_id=record.id, parent_task_id=parent_task_id_str)
                raise # Re-raise to trigger rollback in process_batch

        return tasks_to_create


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
                
                tasks_to_create: List[Dict[str, Any]] = []
                processed_record_ids: List[int] = []
                failed_record_ids: List[int] = []

                for record in records:
                    try:
                        task_params_list = await self._process_output_record(record)
                        if task_params_list:
                            tasks_to_create.extend(task_params_list)
                        processed_record_ids.append(record.id)
                    except Exception as e:
                        full_traceback = traceback.format_exc()
                        self.logger.error(
                            f"Error processing output record {record.id}: {e}",
                            record_id=record.id,
                            error_message=str(e),
                            traceback=full_traceback,
                            exc_info=True
                        )
                        failed_record_ids.append(record.id)
                        continue
                
                if tasks_to_create or processed_record_ids or failed_record_ids:
                    self.logger.info(f"Committing Pipeliner batch: {len(tasks_to_create)} new tasks, "
                                   f"{len(processed_record_ids)} records processed, {len(failed_record_ids)} failed.")
                    
                    async with self.db.get_async_session() as session:
                        try:
                            if tasks_to_create:
                                await self.db.create_task_batch(tasks_to_create, session=session)
                            if processed_record_ids:
                                await self.db.update_output_record_status_batch(processed_record_ids, "PROCESSED", session=session)
                            if failed_record_ids:
                                await self.db.update_output_record_status_batch(failed_record_ids, "FAILED", session=session)
                            await session.commit()
                            self.logger.info("Pipeliner batch transaction committed successfully.")
                        except Exception as batch_error:
                            await session.rollback()
                            self.logger.error("Pipeliner batch transaction failed and was rolled back.", error=str(batch_error), exc_info=True)

                self.orchestrator.update_thread_liveness("pipeliner")
            
            except Exception as e:
                self.logger.error("An unexpected error occurred in the Pipeliner loop.", exc_info=True)
                error = self.orchestrator.error_handler.handle_error(e, "Pipeliner_main_loop")
                if error.error_category == ErrorCategory.FATAL_BUG:
                    raise
                await asyncio.sleep(self.interval * 5) # Longer backoff on error

        self.logger.log_component_lifecycle("Pipeliner", "STOPPED")


    async def _process_output_record(self, record: TaskOutputRecord) -> List[Dict[str, Any]]:
        """
        Main dispatcher that routes a record to the correct handler and returns
        the list of new tasks to be created.
        """
        async with self.db.get_async_session() as session:
            parent_task = await self.db.get_task_by_id(record.task_id, session=session)
            
            if not parent_task or parent_task.status != TaskStatus.COMPLETED:
                self.logger.warning(f"Orphaning output record {record.id} from a non-completed or missing parent task.")
                await self.db.update_output_record_status(record.id, "ORPHANED", session=session)
                await session.commit()
                return []

            output_type = record.output_type

            # --- ROUTING LOGIC ---
            if output_type == "NEW_ENUMERATION_BOUNDARIES":
                return await self._create_new_enumeration_tasks(parent_task, record)
            elif output_type == "DISCOVERED_OBJECTS":
                return await self._create_next_stage_scan_tasks(parent_task, record)
            # Add routing for Policy Job output types here if needed in the future
            else:
                self.logger.warning(f"Unknown TaskOutputRecord type '{output_type}' for record {record.id}.")
                return []

    async def _create_new_enumeration_tasks(self, parent_task: Task, record: TaskOutputRecord) -> List[Dict[str, Any]]:
        """
        Handles the "fan-out" for enumeration. It takes a list of new boundaries
        (subdirectories, schemas, etc.) and batches them into new DISCOVERY_ENUMERATE tasks.
        """
        tasks_to_create = []
        discovered_boundaries = record.output_payload.get("discovered_objects", [])
        if not discovered_boundaries:
            return []

        # This should be a configurable setting.
        batch_size = 50  
        
        parent_work_packet = WorkPacket(**parent_task.work_packet)

        for i in range(0, len(discovered_boundaries), batch_size):
            batch_of_paths = [boundary['object_path'] for boundary in discovered_boundaries[i:i + batch_size]]
            
            task_id_bytes = generate_task_id()
            header = WorkPacketHeader(
                task_id=task_id_bytes.hex(),
                job_id=parent_task.job_id,
                parent_task_id=parent_task.id.hex()
            )
            # The payload contains the new list of paths for the next worker to scan.
            payload = DiscoveryEnumeratePayload(
                datasource_id=parent_work_packet.payload.datasource_id,
                paths=batch_of_paths,
                staging_table_name=parent_work_packet.payload.staging_table_name
            )
            work_packet = WorkPacket(header=header, config=TaskConfig(), payload=payload)

            tasks_to_create.append({
                "job_id": parent_task.job_id,
                "task_id": task_id_bytes,
                "task_type": TaskType.DISCOVERY_ENUMERATE,
                "work_packet": work_packet.model_dump(mode='json'),
                "parent_task_id": parent_task.id
            })
            
        self.logger.info(f"Fanning out {len(discovered_boundaries)} new boundaries into {len(tasks_to_create)} new enumeration tasks.", job_id=parent_task.job_id)
        return tasks_to_create


    async def _create_next_stage_scan_tasks(self, parent_task: Task, record: TaskOutputRecord, context: Dict) -> List[Dict[str, Any]]:
        """
        *** MODIFIED PURPOSE ***
        Creates DISCOVERY_GET_DETAILS tasks for a batch of discovered objects *if*
        the job configuration specifies a 'two-phase' discovery workflow.
        It DOES NOT create CLASSIFICATION tasks anymore.
        """
        job = (await self.db.get_jobs_by_ids([parent_task.job_id], context=context))[0]
        if not job:
            self.logger.error(f"Cannot pipeline task for missing job {parent_task.job_id}", **context)
            return []

        # Check the job's configuration for the discovery workflow type
        workflow = job.configuration.get("discovery_workflow", "single-phase") # Default to single-phase if missing

        # --- Only proceed if it's a two-phase workflow ---
        if workflow != "two-phase":
            self.logger.debug(f"Skipping DISCOVERY_GET_DETAILS task creation for job {job.id} (workflow: {workflow})", **context)
            return [] # Return empty list, no tasks to create

        # --- Proceed with creating DISCOVERY_GET_DETAILS ---
        discovered_object_dicts = record.output_payload.get("discovered_objects", [])
        if not discovered_object_dicts:
            return []

        # Re-instantiate the Pydantic models from the dictionaries
        discovered_objects: List[DiscoveredObject] = []
        for data_dict in discovered_object_dicts:
            # Handle potential base64 encoded hash from JSON transport
            if 'object_key_hash' in data_dict and isinstance(data_dict['object_key_hash'], str):
                 try:
                     data_dict['object_key_hash'] = base64.b64decode(data_dict['object_key_hash'])
                 except Exception:
                     self.logger.warning(f"Could not decode object_key_hash for object path {data_dict.get('object_path')}", **context)
                     continue # Skip object with invalid hash
            # Only add valid objects
            try:
                # Ensure mandatory fields for DiscoveredObject are present
                if all(k in data_dict for k in ['object_key_hash', 'datasource_id', 'object_type', 'object_path']):
                     # Ensure object_type is a valid enum member if needed before validation
                     if isinstance(data_dict.get('object_type'), str):
                         try:
                             data_dict['object_type'] = ObjectType(data_dict['object_type'])
                         except ValueError:
                              self.logger.warning(f"Invalid object_type '{data_dict['object_type']}' for object path {data_dict.get('object_path')}", **context)
                              continue # Skip object with invalid type
                     
                     # Add default discovery_timestamp if missing, Pydantic should handle this
                     data_dict.setdefault('discovery_timestamp', datetime.now(timezone.utc))

                     discovered_objects.append(DiscoveredObject.model_validate(data)) # Use model_validate
                else:
                     self.logger.warning(f"Skipping object due to missing key fields: {data_dict.get('object_path')}", **context)
            except Exception as pydantic_error:
                 self.logger.warning(f"Failed to validate DiscoveredObject data: {pydantic_error}", data=data_dict, **context)
                 continue # Skip invalid object data

        if not discovered_objects:
            self.logger.warning("No valid discovered objects found in output record after validation for GET_DETAILS.", **context)
            return []

        datasource_id = job.configuration.get('datasource_targets', [{}])[0].get('datasource_id')
        if not datasource_id:
            self.logger.error(f"No datasource_id in job configuration for job {job.id}", **context)
            return []

        # --- Create ONLY the DISCOVERY_GET_DETAILS task ---
        tasks_to_create = []
        details_task_id = generate_task_id()
        details_header = WorkPacketHeader(
            task_id=details_task_id.hex(),
            job_id=job.id,
            parent_task_id=parent_task.id.hex()
        )
        # Pass the validated list of DiscoveredObject models
        details_payload = DiscoveryGetDetailsPayload(
            datasource_id=datasource_id,
            discovered_objects=discovered_objects # Pass the Pydantic model list
        )
        details_packet = WorkPacket(header=details_header, config=TaskConfig(), payload=details_payload)

        tasks_to_create.append({
            "job_id": job.id,
            "task_id": details_task_id, # Use bytes
            "task_type": TaskType.DISCOVERY_GET_DETAILS.value, # Use enum value
            "work_packet": details_packet.model_dump(mode='json'), # Serialize Pydantic models correctly
            "parent_task_id": parent_task.id, # Use bytes
            "datasource_id": datasource_id, # Add datasource_id
            "eligible_worker_type": "DATACENTER", # Explicitly set worker type
            "node_group": job.node_group or "default" # Use job's node group
        })

        self.logger.info(f"Pipeliner creating {len(tasks_to_create)} DISCOVERY_GET_DETAILS tasks for {len(discovered_objects)} objects.", **context)
        return tasks_to_create




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
