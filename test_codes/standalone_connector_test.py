# standalone_connector_test.py
import asyncio
import logging
import json
from typing import List, Union

# --- Core Component Imports ---
from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger,JsonFormatter
from core.credentials.credential_manager import CredentialManager
from worker.worker import ConnectorFactory
from classification.engineinterface import EngineInterface
from core.interfaces.worker_interfaces import IFileDataSourceConnector, IDatabaseDataSourceConnector
from core.models.models import (
    TaskType,
    WorkPacket,
    WorkPacketHeader,
    TaskConfig,
    DiscoveredObject,
    DiscoveryEnumeratePayload,
    ClassificationPayload,
    DiscoveryGetDetailsPayload,ObjectType
)
from core.utils.hash_utils import generate_task_id

# =============================================================================
# --- TEST CONFIGURATION ---
# >> Modify this section to change the test case <<
# =============================================================================

# Choose which datasource to test. The script will run the full workflow for it.
TEST_DATASOURCE_ID: str = "ds_local_test_files"  # Options: "ds_local_test_files", "ds_smb_finance_dept", "ds_sql_localhost"

# --- Static Configuration (values from your request) ---
#CLASSIFIER_TEMPLATE_ID: str = "general_corporate_v1.0"
CLASSIFIER_TEMPLATE_ID: str = "unstructured"
CONFIG_FILE_PATH: str = "config/system_default.yaml"
MOCK_JOB_ID: int = 83

# =============================================================================

async def run_test():
    """
    A standalone test harness to simulate a multi-step workflow:
    1. Enumerate objects for a datasource.
    2. Use the results to create and run Classification and Get Details tasks.
    """
    db_interface = None
    print("--- 1. INITIALIZING CORE COMPONENTS ---")
    try:
        # --- Boilerplate setup ---
        # --- WITH THIS NEW LOGGING SETUP ---
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers:
                root_logger.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)
        # --- END OF REPLACEMENT ---
        
        error_handler = ErrorHandler()
        config_manager = ConfigurationManager(CONFIG_FILE_PATH)
        
        mock_ambient_context = {
            "component_name": "StandaloneTest", "machine_name": "test_host",
            "node_group": "default", "deployment_model": "single_process"
        }
        temp_logger = SystemLogger(logging.getLogger("test_startup"), "TEXT", mock_ambient_context)
        
        config_manager.set_core_services(temp_logger, error_handler)
        
        db_interface = DatabaseInterface(config_manager.get_db_connection_string(), temp_logger, error_handler)
        await db_interface.test_connection()
        
        await config_manager.load_database_overrides_async(db_interface)
        config_manager.finalize()
        settings = config_manager.settings
        
        logger = SystemLogger(logging.getLogger(__name__), settings.logging.format, mock_ambient_context)
        db_interface.logger = logger
        
        print(f"--- 2. CREATING ON-DEMAND FACTORIES AND INTERFACES ---")
        credential_manager = CredentialManager()
        connector_factory = ConnectorFactory(
            logger=logger, error_handler=error_handler, db_interface=db_interface,
            credential_manager=credential_manager, system_config=settings
        )
        engine_interface = EngineInterface(
            db_interface=db_interface, config_manager=config_manager, system_logger=logger,
            error_handler=error_handler, job_context={"job_id": MOCK_JOB_ID, "datasource_id": TEST_DATASOURCE_ID}
        )

        # =================================================================
        # --- PHASE 1: DISCOVERY ENUMERATION ---
        # =================================================================
        print(f"\n=== PHASE 1: Running DISCOVERY_ENUMERATE for DATASOURCE: {TEST_DATASOURCE_ID} ===")
        
        task_id_bytes = generate_task_id()
        header = WorkPacketHeader(task_id=task_id_bytes.hex(), job_id=MOCK_JOB_ID)
        payload = DiscoveryEnumeratePayload(
            datasource_id=TEST_DATASOURCE_ID, paths=[],
            staging_table_name="discovered_objects", task_staging_table_name=f"stage_task_outputs_job_{MOCK_JOB_ID}"
        )
        enum_work_packet = WorkPacket(header=header, config=TaskConfig(), payload=payload)
        
        connector = await connector_factory.create_connector(TEST_DATASOURCE_ID)
        print(f"Successfully created connector of type: {type(connector).__name__}")

        all_discovered_objects: List[DiscoveredObject] = []
        async for batch in connector.enumerate_objects(enum_work_packet):
            print(f"\n--- Received DiscoveryBatch (is_final: {batch.is_final_batch}) ---")
            for obj in batch.discovered_objects:
                all_discovered_objects.append(obj)
                print(f"  - Discovered: {obj.object_type.value} at path '{obj.object_path}'")

        print(f"\n--- ENUMERATION COMPLETE ---")
        print(f"Total objects discovered: {len(all_discovered_objects)}")

        if not all_discovered_objects:
            print("\nNo objects were discovered. Cannot proceed to next phases. Exiting.")
            return

# =================================================================
        # --- PHASE 2: PREPARE FOR NEXT-STAGE TASKS ---
        # =================================================================
        
        objects_for_next_stage: List[DiscoveredObject] = []

        # First, check if the initial discovery already found the final objects we need
        # (this handles the connector's "drill-down" optimization).
        for obj in all_discovered_objects:
            if obj.object_type in [ObjectType.DATABASE_TABLE, ObjectType.FILE]:
                objects_for_next_stage.append(obj)

        # If the initial discovery did NOT find tables/files, it might have found schemas.
        # In that case, we need to perform the second-level discovery.
        if not objects_for_next_stage and isinstance(connector, IDatabaseDataSourceConnector):
            print("\n--- Initial discovery found schemas. Running SECOND-LEVEL DISCOVERY to find tables ---")
            
            schema_objects = [obj for obj in all_discovered_objects if obj.object_type == ObjectType.SCHEMA]
            if not schema_objects:
                print("No schemas found to continue discovery.")
            else:
                schema_paths = [obj.object_path for obj in schema_objects]
                print(f"Found {len(schema_paths)} schemas. Now searching for tables within them...")

                # Create and run a new enumeration task for the discovered schemas
                second_task_id_bytes = generate_task_id()
                second_header = WorkPacketHeader(task_id=second_task_id_bytes.hex(), job_id=MOCK_JOB_ID)
                second_payload = DiscoveryEnumeratePayload(
                    datasource_id=TEST_DATASOURCE_ID,
                    paths=schema_paths,
                    staging_table_name="discovered_objects",
                    task_staging_table_name=f"stage_task_outputs_job_{MOCK_JOB_ID}"
                )
                second_work_packet = WorkPacket(header=second_header, config=TaskConfig(), payload=second_payload)
                
                async for batch in connector.enumerate_objects(second_work_packet):
                    for obj in batch.discovered_objects:
                        if obj.object_type == ObjectType.DATABASE_TABLE:
                            objects_for_next_stage.append(obj)
                            print(f"  - Discovered Table: {obj.object_path}")
        
        if not objects_for_next_stage:
            print("\nNo classifiable objects (files or tables) found after all discovery phases. Cannot proceed.")
            return

        # Take a sample of up to 2 objects for the classification and get_details tasks
        objects_for_next_stage = objects_for_next_stage[:2]
        print(f"\nProceeding to next phases with {len(objects_for_next_stage)} objects.")
        

        
        # --- 2.1: Classification Task ---
        print(f"\n=== PHASE 2.1: Running CLASSIFICATION for {len(objects_for_next_stage)} objects ===")
        class_task_id_bytes = generate_task_id()
        class_header = WorkPacketHeader(task_id=class_task_id_bytes.hex(), job_id=MOCK_JOB_ID)
        class_payload = ClassificationPayload(
            datasource_id=TEST_DATASOURCE_ID,
            classifier_template_id=CLASSIFIER_TEMPLATE_ID,
            discovered_objects=objects_for_next_stage
        )
        class_work_packet = WorkPacket(header=class_header, config=TaskConfig(), payload=class_payload)

        await engine_interface.initialize_for_template(CLASSIFIER_TEMPLATE_ID)

        total_findings = 0
        # --- FIX: Use different logic based on the connector's interface ---
        if isinstance(connector, IFileDataSourceConnector):
            print(f"\n=== PHASE 2.1:IFileDataSourceConnector Running CLASSIFICATION for {len(objects_for_next_stage)} objects ===")            
            print("\n--- Processing via File Connector Logic (component by component) ---")
            async for component in connector.get_object_content(class_work_packet):
                print(f"\n  -> Classifying Component: {component.component_id} (Type: {component.component_type})")
                findings = await engine_interface.classify_document_content(component.content, {"file_path": component.parent_path})
                if findings:
                    total_findings += len(findings)
                    for finding in findings:
                        print(f"     [FOUND] Type: {finding.entity_type}, Text: \"{finding.text}\", Score: {finding.confidence_score:.2f}")

        elif isinstance(connector, IDatabaseDataSourceConnector):
            print("\n--- Processing via Database Connector Logic (row by row) ---")
            # The database connector yields one 'content_batch' dictionary per table
            async for content_batch in connector.get_object_content(class_work_packet):
                metadata = content_batch.get("metadata", {})
                rows = content_batch.get("content", [])
                
                print(f"\n  -> Classifying Table: {metadata.get('object_path')} ({len(rows)} rows sampled)")
                
                for row_index, row_data in enumerate(rows):
                    row_pk = {"row_index": row_index, "object_path": metadata.get("object_path")}
                    
                    # Use the correct method for classifying structured database rows
                    row_findings = await engine_interface.classify_database_row(
                        row_data=row_data,
                        row_pk=row_pk,
                        table_metadata=metadata
                    )
                    
                    if row_findings:
                        total_findings += len(row_findings)
                        print(f"     [FOUND] {len(row_findings)} findings in row {row_index}:")
                        for finding in row_findings:
                            field_name = finding.context_data.get('field_name', 'unknown')
                            print(f"       - Type: {finding.entity_type}, Text: \"{finding.text}\", Field: '{field_name}'")
        else:
            print(f"\n[ERROR] Unknown connector type: {type(connector).__name__}")
        # --- END FIX ---

        print(f"\n--- CLASSIFICATION COMPLETE ---")
        print(f"Total PII findings: {total_findings}")
        # --- 2.2: Get Details Task ---
        print(f"\n=== PHASE 2.2: Running DISCOVERY_GET_DETAILS for {len(objects_for_next_stage)} objects ===")
        details_task_id_bytes = generate_task_id()
        details_header = WorkPacketHeader(task_id=details_task_id_bytes.hex(), job_id=MOCK_JOB_ID)
        details_payload = DiscoveryGetDetailsPayload(
            datasource_id=TEST_DATASOURCE_ID,
            discovered_objects=objects_for_next_stage
        )
        details_work_packet = WorkPacket(header=details_header, config=TaskConfig(), payload=details_payload)

        metadata_results = await connector.get_object_details(details_work_packet)
        for metadata in metadata_results:
            # Use dot notation (.) to access attributes of the Pydantic model object
            print(f"\n  -> Details for Object: {metadata.base_object.object_path}")
            
            # Use dot notation here as well
            print("     " + json.dumps(metadata.detailed_metadata, indent=6).replace('\n', '\n     '))
        # --- FIX ENDS HERE ---

        print(f"\n--- GET DETAILS COMPLETE ---")
        print(f"Fetched details for {len(metadata_results)} objects.")

    except Exception as e:
        print(f"\nFATAL ERROR DURING TEST: {e}")
        logging.exception("An unhandled exception occurred.")
    finally:
        if db_interface:
            await db_interface.close_async()
        print("\n--- SCRIPT FINISHED ---")

if __name__ == "__main__":
    asyncio.run(run_test())