# src/standalone_test.py
import asyncio
import os
import logging
import json

# Core components from your application
from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger, JsonFormatter
from content_extraction.content_extractor import ContentExtractor
from classification.engineinterface import EngineInterface

# --- Configuration for the test ---
ROOT_TEST_DIR = "C:/TestDataClassification/test_files"
CLASSIFIER_TEMPLATE_ID = "general_corporate_v1.0"
CONFIG_FILE_PATH = "config/system_default.yaml"

async def run_test():
    """
    Initializes the system's core components and runs the extraction and
    classification pipeline on a local directory.
    """
    db_interface = None
    print("--- 1. INITIALIZING CORE COMPONENTS ---")
    try:
        # --- Boilerplate setup (similar to main.py) ---
        logging.basicConfig(level="INFO", format='%(asctime)s - %(message)s')
        
        error_handler = ErrorHandler()
        temp_logger = SystemLogger(logging.getLogger("test_startup"), "TEXT", {})
        
        config_manager = ConfigurationManager(CONFIG_FILE_PATH)
        config_manager.set_core_services(temp_logger, error_handler)
        
        db_interface = DatabaseInterface(config_manager.get_db_connection_string(), temp_logger, error_handler)
        await db_interface.test_connection()
        
        await config_manager.load_database_overrides_async(db_interface)
        config_manager.finalize()
        settings = config_manager.settings
        
        # Use a final, properly configured logger
        logger = SystemLogger(logging.getLogger(__name__), settings.logging.format, config_manager.get_partial_ambient_context())
        db_interface.logger = logger
        
        print(f"--- 2. INITIALIZING PIPELINE FOR TEMPLATE: {CLASSIFIER_TEMPLATE_ID} ---")
        
        # --- Instantiate the main processing components ---
        
        # A mock job_context, as this would normally come from the Orchestrator
        mock_job_context = {
            "job_id": "standalone_test_job",
            "task_id": "standalone_test_task",
            "datasource_id": "standalone_test_ds",
            "trace_id": "standalone_test_trace"
        }
        
        # The Content Extractor, which handles reading and parsing files
        content_extractor = ContentExtractor(
            extraction_config=settings.content_extraction,
            system_config=settings,
            logger=logger,
            error_handler=error_handler
        )
        
        # The Engine Interface, which handles the classification logic
        engine_interface = EngineInterface(
            db_interface=db_interface,
            config_manager=config_manager,
            system_logger=logger,
            error_handler=error_handler,
            job_context=mock_job_context
        )
        await engine_interface.initialize_for_template(CLASSIFIER_TEMPLATE_ID)
        
        print(f"\n--- 3. STARTING FILE SCAN IN: {ROOT_TEST_DIR} ---\n")

        # --- Execution loop ---
        for root, _, files in os.walk(ROOT_TEST_DIR):
            for filename in files:
                file_path = os.path.join(root, filename)
                object_id = file_path # For a local test, the path is a sufficient ID
                
                print(f"--- Processing File: {file_path} ---")
                
                all_findings_for_file = []
                
                try:
                    # 1. Use ContentExtractor to get all parts of the file
                    async for component in content_extractor.extract_from_file(file_path, object_id):
                        print(f"  -> Extracted Component: {component.component_id} (Type: {component.component_type})")
                        
                        # 2. Classify each component using the EngineInterface
                        if component.component_type == "table":
                            table_rows = component.schema.get("rows", [])
                            table_metadata = {"table_name": component.component_id, "columns": {h: {} for h in component.schema.get("headers", [])}}
                            for i, row_data in enumerate(table_rows):
                                row_pk = {"row_index": i}
                                findings = await engine_interface.classify_database_row(row_data, row_pk, table_metadata)
                                all_findings_for_file.extend(findings)
                        
                        elif component.component_type in ["text", "image_ocr", "archive_member"]:
                            file_metadata = {"file_path": file_path}
                            findings = await engine_interface.classify_document_content(component.content, file_metadata)
                            all_findings_for_file.extend(findings)

                except Exception as e:
                    print(f"  [ERROR] Could not process file {file_path}: {e}")

                # 3. Report the findings for the file
                if all_findings_for_file:
                    print(f"\n  [FOUND] {len(all_findings_for_file)} PII FINDINGS IN THIS FILE:")
                    for finding in all_findings_for_file:
                        print(f"    - Type: {finding.entity_type}, Text: \"{finding.text}\", Score: {finding.confidence_score:.2f}")
                else:
                    print("  [OK] No findings in this file.")
                print("-" * (len(file_path) + 22))

    except Exception as e:
        print(f"\nFATAL ERROR DURING INITIALIZATION: {e}")
    finally:
        if db_interface:
            await db_interface.close_async()
        print("\n--- TEST COMPLETE ---")

if __name__ == "__main__":
    # This allows the script to run standalone
    asyncio.run(run_test())