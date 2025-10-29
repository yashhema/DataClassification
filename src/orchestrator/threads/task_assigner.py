# src/orchestrator/threads/task_assigner.py
"""
The TaskAssigner is the core logic coroutine of the Orchestrator. It is
responsible for matching pending tasks with available resources and workers,
implementing a robust scheduling, caching, and leasing strategy.

FIXES APPLIED:
- Updated to use unified job_cache from orchestrator (no separate task cache)
- Fixed state access to work with new unified dictionary structure
- Fixed schema compliance: uses task.datasource_id and task.job_id
- Updated to use existing get_jobs_by_ids database method
"""

import asyncio
import time
import base64
from collections import deque
from datetime import datetime, timezone,timedelta

# Import for type hinting
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from orchestrator.orchestrator import Orchestrator

# Import from the neutral state file
from orchestrator.orchestrator_state import JobState

from core.db_models.job_schema import JobType
from core.models.models import TaskType, PolicySelectorPlanPayload, PolicyConfiguration, WorkPacketHeader, TaskConfig, WorkPacket, DiscoveryEnumeratePayload,BenchmarkExecutePayload,EntitlementExtractPayload,DatasourceProfilePayload
from core.errors import ErrorCategory
from core.utils.hash_utils import generate_task_id

class TaskAssigner:
    """Finds, approves, and dispatches tasks, respecting all system constraints."""
    
    def __init__(self, orchestrator: "Orchestrator"):
        self.orchestrator = orchestrator
        self.logger = orchestrator.logger
        self.db = orchestrator.db
        self.rc = orchestrator.resource_coordinator
        self.settings = orchestrator.settings
        self.interval = orchestrator.settings.orchestrator.task_assigner_interval_sec
        self.name = "TaskAssignerCoroutine"
        
        # NOTE: Using orchestrator's unified job_cache instead of separate cache
        # All task caching operations will use self.orchestrator.job_cache


    async def run_async(self):
        """The main async loop for the Task Assigner coroutine."""
        self.logger.log_component_lifecycle("TaskAssigner", "STARTED")
        backoff_delay = self.interval

        while not self.orchestrator._shutdown_event.is_set():
            try:
                # BACKPRESSURE CHECK (for single_process mode)
                if self.orchestrator.is_single_process_mode:
                    q_size = self.orchestrator._in_process_work_queue.qsize()
                    # Threshold should be configurable, e.g., 2x worker count
                    if q_size > (self.settings.worker.in_process_thread_count * 2):
                        self.logger.warning(f"Task queue is full ({q_size} items). Throttling task assignment.", queue_size=q_size)
                        await asyncio.sleep(backoff_delay)
                        backoff_delay = min(backoff_delay * 1.5, 60) # Exponential backoff up to 60s
                        continue
                
                backoff_delay = self.interval # Reset backoff on successful check

                # Continuously try to find and dispatch the next available task
                task_to_dispatch = await self._find_and_approve_task()
                if task_to_dispatch:
                    await self._dispatch_task(task_to_dispatch)
                else:
                    await asyncio.sleep(self.interval)
                
                self.orchestrator.update_thread_liveness("task_assigner")
            
            except Exception as e:
                error = self.orchestrator.error_handler.handle_error(e, "task_assigner_main_loop")
                
                # Fatal error detection and propagation
                if error.error_category == ErrorCategory.FATAL_BUG:
                    self.logger.critical(f"Fatal error detected in {self.__class__.__name__}: {error}")
                    raise  # Propagate to TaskManager
                
                # Existing retry logic for operational errors
                self.logger.warning(f"Transient error in {self.__class__.__name__}, retrying: {error}")
                await asyncio.sleep(self.interval * 5)

        self.logger.log_component_lifecycle("TaskAssigner", "STOPPED")


    async def _claim_and_initialize_new_job(self) -> bool:
        """
        Finds a single QUEUED job, checks orchestrator load balance, claims the job if appropriate,
        and creates the correct first task with an application-generated hash ID.
        Returns True if a job was successfully claimed and initialized, False otherwise.
        """
        nodegroup = self.settings.system.node_group
        context = {"nodegroup": nodegroup, "orchestrator_id": self.orchestrator.instance_id}
        proceed_with_claim = True # Flag to control claim attempt

        # --- LOAD BALANCING CHECK ---
        try:
            # Query DB for load counts of *currently active* orchestrators
            # Use a threshold slightly longer than the LeaseManager interval
            active_loads = await self.db.get_active_orchestrator_loads(
                heartbeat_threshold_seconds=self.load_check_heartbeat_threshold,
                context=context
            )
            own_load = 0
            other_loads = []
            for load_info in active_loads:
                if load_info['orchestrator_id'] == self.orchestrator.instance_id:
                    own_load = load_info['active_job_count']
                else:
                    other_loads.append(load_info['active_job_count'])

            # Balancing logic: Skip if own load is > min other load + threshold
            min_other_load = min(other_loads) if other_loads else 0
            load_threshold = 2 # Configurable: how many more jobs is "too many"

            # Only apply balancing if more than one orchestrator is active
            if len(active_loads) > 1 and own_load > min_other_load + load_threshold:
                self.logger.info(f"Skipping job claim attempt due to load imbalance.",
                                 own_load=own_load, min_other_load=min_other_load, threshold=load_threshold, **context)
                proceed_with_claim = False # Set flag to skip claim

        except Exception as load_check_error:
            # If checking load fails, log warning but proceed cautiously (attempt claim anyway)
            self.orchestrator.error_handler.handle_error(load_check_error, "check_orchestrator_load_balance", **context)
            self.logger.warning("Failed to check orchestrator load balance. Proceeding cautiously with claim attempt.", **context)
            # proceed_with_claim remains True (fallback behavior)
        # --- END LOAD BALANCING CHECK ---

        # --- Proceed only if load balancing allows ---
        if not proceed_with_claim:
            return False # Skipped due to load

        # --- Find and attempt to claim a job ---
        queued_jobs = await self.db.get_queued_jobs_for_nodegroup(nodegroup, limit=1, context=context)
        if not queued_jobs:
            return False # No new jobs available

        job_to_claim = queued_jobs[0]
        job_id = job_to_claim.id
        context["job_id"] = job_id # Add job_id to context after selection
        context["template_type"] = job_to_claim.template_type.value # Add type for logging

        lease_duration = 300 # Should be configurable via self.settings
        was_claimed = await self.db.claim_queued_job(
            job_id, self.orchestrator.instance_id, lease_duration, context=context
        )

        # --- If claim successful, initialize the first task ---
        if was_claimed:
            self.logger.info(
                f"Successfully claimed new '{job_to_claim.template_type.value}' job {job_id}.",
                **context
            )
            # Update in-memory state after successful claim
            await self.orchestrator._update_job_in_memory_state(
                job_id=job_id,
                new_status=JobState.RUNNING,
                version=job_to_claim.version + 1, # DB version implicitly increments on claim
                lease_expiry=datetime.now(timezone.utc) + timedelta(seconds=lease_duration),
                is_new=True
            )

            # --- Create the FIRST task based on Job Type ---
            new_task_id_bytes = generate_task_id()
            new_task_id_hex = new_task_id_bytes.hex()
            task_type = None
            payload = None
            eligible_worker_type = "DATACENTER" # Default
            target_node_group = job_to_claim.node_group or "default"

            try:
                # Route to appropriate payload preparation helper
                if job_to_claim.template_type in [JobType.SCANNING, JobType.DB_PROFILE, JobType.BENCHMARK, JobType.ENTITLEMENT, JobType.VULNERABILITY]:
                    task_type, payload, eligible_worker_type, target_node_group = await self._prepare_first_scan_task_payload(job_to_claim, new_task_id_hex, context)
                elif job_to_claim.template_type == JobType.POLICY:
                    task_type, payload, eligible_worker_type, target_node_group = await self._prepare_first_policy_task_payload(job_to_claim, new_task_id_hex, context)
                else:
                    raise ValueError(f"Unknown job type '{job_to_claim.template_type.value}' during initial task creation.")

                if task_type and payload:
                    # Build the WorkPacket
                    header = WorkPacketHeader(task_id=new_task_id_hex, job_id=job_id)
                    config = TaskConfig() # Default config
                    # Ensure payload is correctly instantiated if helpers return models
                    if isinstance(payload, BaseModel):
                         payload_model = payload
                    else:
                         # Attempt to create model if helpers returned dict (adjust based on helper return type)
                         payload_model = self._get_payload_model(task_type)(**payload)

                    full_work_packet = WorkPacket(header=header, config=config, payload=payload_model)

                    # Create the Task in DB
                    await self.db.create_task(
                        job_id=job_id,
                        task_id=new_task_id_bytes,
                        task_type=task_type.value,
                        work_packet=full_work_packet.model_dump(mode='json'),
                        datasource_id=getattr(payload_model, 'datasource_id', None),
                        eligible_worker_type=eligible_worker_type,
                        node_group=target_node_group,
                        context=context
                    )
                    self.logger.info(f"Created initial task '{task_type.value}' ({new_task_id_hex}) for job {job_id}", **context)

                    # Refill cache immediately after creating the first task
                    await self._refill_task_cache_for_job(job_id)

                    # *** IMPORTANT: Immediately update own load count after successful claim ***
                    try:
                         async with self.orchestrator._state_lock:
                             current_owned_count = len(self.orchestrator.job_states)
                         await self.db.update_orchestrator_load(
                             orchestrator_id=self.orchestrator.instance_id,
                             job_count=current_owned_count,
                             context=context
                         )
                    except Exception as update_err:
                         self.orchestrator.error_handler.handle_error(update_err, "post_claim_load_update", **context)

                    return True # Job claimed and first task created
                else:
                    # If payload creation failed, fail the job
                    await self.db.fail_job(job_id, "Failed to create initial task payload.", context=context)
                    return False

            except Exception as e:
                # Catch errors during task creation, fail the job, and log
                self.orchestrator.error_handler.handle_error(e, "claim_initialize_create_task", **context)
                await self.db.fail_job(job_id, f"Error creating initial task: {str(e)}", context=context)
                return False # Indicate job claim failed overall

        return False # Job wasn't claimed (either due to load or race condition)



    async def _find_and_approve_task(self):
        """
        Finds a runnable job, gets a pending task from its cache, 
        and reserves resources for it.
        """
        # This function now correctly identifies a job and ensures its cache is full.
        job_to_run = await self._select_next_job_to_process()
        
        if not job_to_run: 
            return None # No runnable jobs were found or could be initialized.

        # --- FIX STARTS HERE ---
        # The cache for job_to_run should have been refilled by the selection logic.
        # Now, we simply get a task from it.
        task = self._get_task_from_cache(job_to_run.id)
        
        if not task:
            # This can happen if the refill found no PENDING tasks for the job.
            # The job might be finishing up. This is a normal condition.
            return None

        # Verify the lease one last time before dispatching.
        async with self.orchestrator._state_lock:
            job_state_info = self.orchestrator.job_states.get(job_to_run.id)
            if not job_state_info or job_state_info.get('lease_expiry') < datetime.now(timezone.utc):
                self.logger.warning(f"In-memory lease expired for job {job_to_run.id} just before task dispatch.", job_id=job_to_run.id)
                self.orchestrator.job_cache[job_to_run.id].appendleft(task) # Put task back
                return None

        # Now proceed with resource reservation as before.
        datasource_id_to_check = task.datasource_id
        if not datasource_id_to_check:
            try:
                datasource_id_to_check = task.work_packet['payload']['datasource_ids'][0]
            except (KeyError, IndexError):
                datasource_id_to_check = None
        
        decision_approved = False
        decision_reason = "No datasource-specific resources required."

        if datasource_id_to_check:
            decision = await self.rc.reserve_resources_for_task(datasource_id_to_check)
            decision_approved = decision.is_approved
            decision_reason = decision.reason
        else:
            decision_approved = True

        if decision_approved:
            return task
        else:
            self.orchestrator.job_cache[job_to_run.id].appendleft(task) # Put task back
            async with self.orchestrator._state_lock:
                self.orchestrator.snoozed_jobs[job_to_run.id] = time.monotonic() + 60
            self.logger.info(f"Snoozing job {job_to_run.id}: {decision_reason}", job_id=job_to_run.id)
            return None

    def _get_task_from_cache(self, job_id: int):
        """Gets a single task from the orchestrator's unified cache for a given job."""
        if job_id in self.orchestrator.job_cache and self.orchestrator.job_cache[job_id]:
            return self.orchestrator.job_cache[job_id].popleft()
        return None

    async def _refill_task_cache_for_job(self, job_id: int):
        """Fetches a new batch of pending tasks from the DB for a specific job."""
        async with self.orchestrator._state_lock:
            # Double-check that the cache is still empty to avoid a race condition
            if not (job_id in self.orchestrator.job_cache and self.orchestrator.job_cache[job_id]):
                tasks = await self.db.get_pending_tasks_batch(job_id, self.settings.orchestrator.task_cache_size)
                if tasks:
                    self.orchestrator.job_cache[job_id] = deque(tasks)
                    self.logger.info(f"Refilled task cache for job {job_id} with {len(tasks)} tasks.", job_id=job_id)


    async def _dispatch_task(self, task):
        """Assigns a task in the DB and dispatches it."""
        # Handle both bytes and string task_id
        task_id_str = task.id.hex() if isinstance(task.id, bytes) else task.id
        task_id_bytes = task.id if isinstance(task.id, bytes) else bytes.fromhex(task.id)
        
        worker_id = f"worker_for_task_{task_id_str}"
        
        was_assigned = await self.db.assign_task_to_worker(
            task_id_bytes,
            worker_id,
            self.settings.worker.task_timeout_seconds
        )
        
        if was_assigned:
            # --- START OF CRITICAL FIX ---
            # The task object from the DB has the correct, incremented retry_count.
            # We must now inject this into the work_packet's config before dispatching it.
            # This is the ONLY way the worker knows it's a recovery operation.
            task.work_packet['config']['retry_count'] = task.retry_count
            # --- END OF CRITICAL FIX ---

            self.rc.state_manager.confirm_task_assignment(task.job_id)
            self.logger.info(f"Task {task_id_str} successfully assigned.", task_id=task_id_str)
            
            if self.orchestrator.is_single_process_mode:
                if self.orchestrator._in_process_work_queue is None:
                    self.logger.error("In-process work queue is None!")
                    return                
                await self.orchestrator._in_process_work_queue.put(task)
        else:
            self.logger.warning(f"DB assignment for task {task_id_str} failed (race condition).", task_id=task_id_str)
            if hasattr(task, 'datasource_id') and task.datasource_id:
                self.rc.state_manager.release_datasource_connection(task.datasource_id)


    async def _select_next_job_to_process(self):
        """
        Selects the best job to assign a task from using a capacity-aware,
        pull-based model with consolidated state locking for efficiency.
        """
        # STEP 1: Consolidate state access into a single lock acquisition.
        async with self.orchestrator._state_lock:
            now = time.monotonic()
            
            # Get the current list of owned, runnable jobs.
            owned_job_ids = [
                job_id for job_id, state in self.orchestrator.job_states.items()
                if state.get('status') == JobState.RUNNING and self.orchestrator.snoozed_jobs.get(job_id, 0) < now
            ]
            
            # Calculate the current workload based on tasks already in memory.
            current_workload = sum(len(cache) for job_id, cache in self.orchestrator.job_cache.items() if job_id in owned_job_ids)
            
            # Define the capacity threshold.
            workload_threshold = self.settings.orchestrator.task_cache_size * 2

        # STEP 2: Decide whether to claim a new job based on capacity.
        job_was_claimed = False
        if not owned_job_ids or current_workload < workload_threshold:
            # The method returns True if a new job was successfully claimed and initialized.
            job_was_claimed = await self._claim_and_initialize_new_job()

        # STEP 3: If a new job was claimed, we must refresh our view of owned jobs.
        if job_was_claimed:
            async with self.orchestrator._state_lock:
                owned_job_ids = [
                    job_id for job_id, state in self.orchestrator.job_states.items()
                    if state.get('status') == JobState.RUNNING
                ]

        if not owned_job_ids:
            return None # No work to do.

        active_jobs = await self.db.get_jobs_by_ids(owned_job_ids)
        if not active_jobs:
            return None
            
        # STEP 4: Refill ALL empty caches in parallel.
        refill_coroutines = []
        for job in active_jobs:
            # Check if cache is missing or empty.
            if not self.orchestrator.job_cache.get(job.id):
                refill_coroutines.append(self._refill_task_cache_for_job(job.id))
        
        if refill_coroutines:
            self.logger.info(f"Refilling task caches for {len(refill_coroutines)} jobs in parallel.")
            await asyncio.gather(*refill_coroutines)

        # STEP 5: With caches now full, select the next task fairly.
        jobs_with_tasks_in_cache = [
            job for job in active_jobs if self.orchestrator.job_cache.get(job.id)
        ]

        if jobs_with_tasks_in_cache:
            return self.rc.get_next_job_for_assignment(jobs_with_tasks_in_cache)

        return None    
        
    # --- ***  HELPER METHODS *** ---

    async def _prepare_first_scan_task_payload(self, job: Job, task_id_hex: str, context: Dict) -> Tuple[Optional[TaskType], Optional[BaseModel], str, str]:
        """Helper to build the payload for the first task of various Scan jobs."""
        datasource_id = job.configuration.get('datasource_targets', [{}])[0].get('datasource_id')
        if not datasource_id:
            raise ConfigurationError(f"Job {job.id} is missing a datasource_id.", config_key="datasource_targets")

        task_type = None
        payload = None
        eligible_worker_type = "DATACENTER" # Scan tasks run on datacenter workers
        target_node_group = job.node_group or "default" # Target worker in job's node group

        if job.template_type == JobType.SCANNING:
            task_type = TaskType.DISCOVERY_ENUMERATE
            discovery_mode = job.configuration.get("discovery_mode", "FULL").upper()
            staging_table = None
            if discovery_mode == "DELTA":
                 staging_table = f"staging_discovered_objects_job_{job.id}"
                 # Ensure staging table exists (idempotent creation)
                 await self.db.create_staging_table_for_job(job.id, "DiscoveredObjects", context=context)

            payload = DiscoveryEnumeratePayload(
                datasource_id=datasource_id,
                paths=[], # Initial task starts enumeration from root(s) defined in connector/config
                staging_table_name=staging_table # Will be None for FULL scan mode
            )
        elif job.template_type == JobType.DB_PROFILE:
            task_type = TaskType.DATASOURCE_PROFILE
            payload = DatasourceProfilePayload(datasource_id=datasource_id)
        elif job.template_type == JobType.BENCHMARK:
            task_type = TaskType.BENCHMARK_EXECUTE
            benchmark_name = job.configuration.get("benchmark_name")
            if not benchmark_name:
                 raise ConfigurationError(f"Benchmark job {job.id} is missing 'benchmark_name'.")
            payload = BenchmarkExecutePayload(
                 datasource_id=datasource_id,
                 cycle_id=job.configuration.get("cycle_id", f"cycle_{job.execution_id}"), # Generate cycle ID if needed
                 benchmark_name=benchmark_name
            )
        elif job.template_type == JobType.ENTITLEMENT:
             task_type = TaskType.ENTITLEMENT_EXTRACT
             payload = EntitlementExtractPayload(
                 datasource_id=datasource_id,
                 cycle_id=job.configuration.get("cycle_id", f"cycle_{job.execution_id}") # Generate cycle ID if needed
             )
        # elif job.template_type == JobType.VULNERABILITY:
             # Add logic for VULNERABILITY if implemented
             # task_type = TaskType.VULNERABILITY_SCAN
             # payload = VulnerabilityScanPayload(...)
        else:
             # Should not happen if claim logic is correct, but handles unexpected types
             raise ValueError(f"Unsupported Scan JobType '{job.template_type.value}' in _prepare_first_scan_task_payload for job {job.id}")

        return task_type, payload, eligible_worker_type, target_node_group


    async def _prepare_first_policy_task_payload(self, job: Job, task_id_hex: str, context: Dict) -> Tuple[Optional[TaskType], Optional[BaseModel], str, str]:
        """Helper to build the payload for the first POLICY_QUERY_EXECUTE task."""
        eligible_worker_type = "POLICY" # Policy queries run centrally by PolicyWorker
        target_node_group = "central"   # Always target the central node group

        # Fetch the Policy Template using the job's template_table_id
        # Use caching version for potentially frequent lookups
        policy_template = await self.db.get_policy_template_by_id(job.template_table_id, context=context)
        if not policy_template:
            raise ConfigurationError(f"Policy template ID '{job.template_table_id}' not found for job {job.id}.", config_key="template_table_id")

        # --- Logic moved from Pipeliner ---
        config = policy_template.configuration # This is already a dict from JSON field

        # Determine query source (Athena or Yugabyte replica)
        if config.get("target_query_engine"):
            query_source = config["target_query_engine"]
        else:
            # Infer based on where the data primarily resides
            query_source = "athena" if config.get("policy_storage_scope") in ("S3", "BOTH") else "yugabyte"

        # Get selection criteria and action definition (assuming they are dicts)
        selection_criteria_dict = config.get("selection_criteria")
        action_definition_dict = config.get("action_definition")
        data_source_enum = config.get("data_source") # e.g., "SCANFINDINGS"
        storage_scope_enum = config.get("policy_storage_scope") # e.g., "S3"

        if not selection_criteria_dict or not action_definition_dict or not data_source_enum or not storage_scope_enum:
             raise ConfigurationError(f"Policy template '{policy_template.template_id}' (DB ID: {policy_template.id}) is missing required configuration fields (selection_criteria, action_definition, data_source, or policy_storage_scope).")

        # Validate/Parse into Pydantic models
        try:
            # Ensure enums are handled correctly if stored as strings
            selection_criteria_model = SelectionCriteria.model_validate(selection_criteria_dict)
            action_definition_model = ActionDefinition.model_validate(action_definition_dict)
            # Add validation for data_source and storage_scope if needed, Pydantic handles Literal validation
        except Exception as validation_error:
             raise ConfigurationError(f"Invalid selection_criteria or action_definition in template '{policy_template.template_id}': {validation_error}")

        payload = PolicyQueryExecutePayload(
            policy_template_id=policy_template.id, # Use DB primary key for internal reference
            policy_name=policy_template.name,
            query_source=query_source,
            data_source=data_source_enum, # Pass validated enum/string
            policy_storage_scope=storage_scope_enum, # Pass validated enum/string
            selection_criteria=selection_criteria_model, # Use validated Pydantic model
            action_definition=action_definition_model, # Use validated Pydantic model
            incremental_mode=config.get("policy_execution_type") == "INCREMENTAL",
            incremental_checkpoints=None # PolicyWorker will load these from CheckpointRepository
        )
        # --- End of moved logic ---

        return TaskType.POLICY_QUERY_EXECUTE, payload, eligible_worker_type, target_node_group

    # --- Helper to get Pydantic model class from TaskType ---
    def _get_payload_model(self, task_type: TaskType) -> Optional[type[BaseModel]]:
         """Maps TaskType enum to the corresponding Pydantic payload model class."""
         # This map needs to be kept in sync with core/models/models.py payload definitions
         payload_class_map = {
             TaskType.DISCOVERY_ENUMERATE: DiscoveryEnumeratePayload,
             TaskType.DISCOVERY_GET_DETAILS: DiscoveryGetDetailsPayload,
             TaskType.CLASSIFICATION: ClassificationPayload,
             TaskType.DELTA_CALCULATE: DeltaCalculatePayload,
             TaskType.POLICY_SELECTOR_PLAN: PolicySelectorPlanPayload,
             TaskType.POLICY_SELECTOR_EXECUTE: PolicySelectorExecutePayload,
             TaskType.POLICY_COMMIT_PLAN: PolicyCommitPlanPayload,
             TaskType.POLICY_ENRICHMENT: PolicyEnrichmentPayload,
             TaskType.POLICY_ACTION_EXECUTE: PolicyActionExecutePayload,
             TaskType.POLICY_RECONCILE: PolicyReconcilePayload,
             TaskType.DATASOURCE_PROFILE: DatasourceProfilePayload,
             TaskType.BENCHMARK_EXECUTE: BenchmarkExecutePayload,
             TaskType.ENTITLEMENT_EXTRACT: EntitlementExtractPayload,
             # TaskType.VULNERABILITY_SCAN: VulnerabilityScanPayload, # Add when implemented
             TaskType.POLICY_QUERY_EXECUTE: PolicyQueryExecutePayload, # From core/models/payloads.py
             # TaskType.PREPARE_CLASSIFICATION_TASKS: PrepareClassificationTasksPayload, # From core/models/payloads.py
             # TaskType.JOB_CLOSURE: JobClosurePayload # From core/models/payloads.py
         }
         return payload_class_map.get(task_type)