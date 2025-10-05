# standalone_worker_test.py
import asyncio
import logging
import json
from typing import List, Any, Optional, Dict

# --- Core Component Imports ---

from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger, JsonFormatter
from core.credentials.credential_manager import CredentialManager
from worker.worker import Worker, ConnectorFactory # Import the Worker
from core.models.models import (
    TaskType,
    WorkPacket,
    WorkPacketHeader,
    TaskConfig,
    DiscoveredObject,
    DiscoveryEnumeratePayload,
    ClassificationPayload,
    DiscoveryGetDetailsPayload,
    ObjectType
)
from core.utils.hash_utils import generate_task_id

# =============================================================================
# --- TEST CONFIGURATION ---
# >> Modify this section to change the test case <<
# =============================================================================

# Choose which datasource to test.
TEST_DATASOURCE_ID: str = "ds_local_test_files" # Options: "ds_local_test_files", "ds_smb_finance_dept", "ds_sql_localhost"

# Choose which task type to simulate for the worker.
TEST_TASK_TYPE: TaskType = TaskType.DISCOVERY_ENUMERATE # Options: TaskType.DISCOVERY_ENUMERATE, TaskType.CLASSIFICATION, TaskType.DISCOVERY_GET_DETAILS

# --- Static Configuration ---
CLASSIFIER_TEMPLATE_ID: str = "general_corporate_v1.0"
CONFIG_FILE_PATH: str = "config/system_default.yaml"
MOCK_JOB_ID: int = 83

# =============================================================================

class MockOrchestrator:
    """A mock orchestrator that logs calls from the worker instead of performing real actions."""
    def __init__(self, logger: SystemLogger):
        self.logger = logger

    async def report_task_result_async(
        self, task_id: str, status: str, is_retryable: bool,
        result_payload: Optional[Dict], context: Optional[Dict] = None
    ):
        self.logger.info(
            "[MOCK ORCHESTRATOR] Worker reported task completion.",
            task_id=task_id, status=status, result_payload=result_payload
        )

async def run_worker_simulation():
    """
    A standalone test harness to simulate a full worker lifecycle for a single task,
    including connector interaction and database inserts.
    """
    db_interface = None
    print("--- 1. INITIALIZING CORE COMPONENTS & MOCKS ---")
    try:
        # --- Logging and Component Initialization (remains the same) ---
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers:
                root_logger.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)
        error_handler = ErrorHandler()
        config_manager = ConfigurationManager(CONFIG_FILE_PATH)
        
        mock_ambient_context = {"component_name": "StandaloneWorkerTest", "machine_name": "test_host", "node_group": "default", "deployment_model": "single_process"}
        logger = SystemLogger(logging.getLogger(__name__), "TEXT", mock_ambient_context)
        config_manager.set_core_services(logger, error_handler)
        db_interface = DatabaseInterface(config_manager.get_db_connection_string(), logger, error_handler)
        await db_interface.test_connection()
        await config_manager.load_database_overrides_async(db_interface)
        config_manager.finalize()
        settings = config_manager.settings
        db_interface.logger = logger
        credential_manager = CredentialManager()
        connector_factory = ConnectorFactory(logger=logger, error_handler=error_handler, db_interface=db_interface, credential_manager=credential_manager, system_config=settings)
        mock_orchestrator = MockOrchestrator(logger)

        print("\n--- 2. CREATING WORKER INSTANCE ---")
        worker = Worker(
            worker_id="test_worker_01", settings=settings, db_interface=db_interface,
            connector_factory=connector_factory, logger=logger, config_manager=config_manager,
            error_handler=error_handler, orchestrator_instance=mock_orchestrator
        )
        print(f"Worker instance '{worker.worker_id}' created.")

        print(f"\n--- 3. CREATING WORK PACKET for task: {TEST_TASK_TYPE.value} ---")
        task_id_bytes = generate_task_id()
        header = WorkPacketHeader(task_id=task_id_bytes.hex(), job_id=MOCK_JOB_ID)
        config = TaskConfig()
        payload = None

        if TEST_TASK_TYPE == TaskType.DISCOVERY_ENUMERATE:
            payload = DiscoveryEnumeratePayload(
                datasource_id=TEST_DATASOURCE_ID, paths=[],
                staging_table_name="discovered_objects", task_staging_table_name=f"stage_task_outputs_job_{MOCK_JOB_ID}"
            )
        elif TEST_TASK_TYPE in [TaskType.CLASSIFICATION, TaskType.DISCOVERY_GET_DETAILS]:
            print(f"Fetching up to 5 discovered objects for datasource '{TEST_DATASOURCE_ID}' to build task...")
            # This call returns SQLAlchemy ORM objects
            objects_from_db = await db_interface.get_objects_by_datasource(TEST_DATASOURCE_ID, limit=5)
            
            if not objects_from_db:
                print(f"[ERROR] No discovered objects found for {TEST_DATASOURCE_ID}. Cannot create task.")
                return
            print(f"Found {len(objects_from_db)} objects from DB. Converting to Pydantic models...")

            # --- FIX STARTS HERE: Convert ORM objects to Pydantic objects ---
            pydantic_objects = []
            for orm_obj in objects_from_db:
                pydantic_obj = DiscoveredObject(
                    object_key_hash=orm_obj.object_key_hash,
                    datasource_id=orm_obj.data_source_id,
                    object_type=ObjectType(orm_obj.object_type),
                    object_path=orm_obj.object_path,
                    size_bytes=orm_obj.size_bytes or 0,
                    created_date=orm_obj.created_date,
                    last_modified=orm_obj.last_modified,
                    last_accessed=orm_obj.last_accessed,
                    discovery_timestamp=orm_obj.discovery_timestamp
                )
                pydantic_objects.append(pydantic_obj)
            # --- FIX ENDS HERE ---

            if TEST_TASK_TYPE == TaskType.CLASSIFICATION:
                payload = ClassificationPayload(
                    datasource_id=TEST_DATASOURCE_ID, classifier_template_id=CLASSIFIER_TEMPLATE_ID,
                    discovered_objects=pydantic_objects # Use the correctly typed list
                )
            else: # DISCOVERY_GET_DETAILS
                payload = DiscoveryGetDetailsPayload(
                    datasource_id=TEST_DATASOURCE_ID, discovered_objects=pydantic_objects # Use the correctly typed list
                )
        
        if not payload:
            print(f"[ERROR] Could not create a payload for the selected task type: {TEST_TASK_TYPE.value}")
            return
            
        work_packet = WorkPacket(header=header, config=config, payload=payload)
        print("Work Packet created successfully.")

        print("\n--- 4. CALLING worker._process_task_async() ---")
        await worker._process_task_async(work_packet)
        
        print("\n--- 5. WORKER TASK PROCESSING COMPLETE ---")
        
    except Exception as e:
        print(f"\nFATAL ERROR DURING TEST: {e}")
        logging.exception("An unhandled exception occurred.")
    finally:
        if db_interface:
            await db_interface.close_async()
        print("\n--- SCRIPT FINISHED ---")

if __name__ == "__main__":
    asyncio.run(run_worker_simulation())