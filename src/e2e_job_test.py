# e2e_job_test.py
"""
End-to-End Job Test Script

Tests the complete job lifecycle: Discovery → Classification → Verification
Uses production Orchestrator components (TaskAssigner, Pipeliner) without starting background coroutines.
"""
import sys

import os
sys.stdout.reconfigure(line_buffering=True)
import asyncio
import logging
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
from typing import Set, Dict, Any
from datetime import datetime, timezone, timedelta

# SQLAlchemy for ground truth gathering
from sqlalchemy import create_engine, text, select, func

# Core Component Imports
from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger, JsonFormatter
from core.credentials.credential_manager import CredentialManager
from worker.worker import Worker, ConnectorFactory
from orchestrator.orchestrator import Orchestrator
from orchestrator.orchestrator_state import JobState
from core.db_models.discovery_catalog_schema import ObjectMetadata
from core.db_models.findings_schema import ScanFindingSummary
from core.db_models.job_schema import Job, JobType, JobStatus
from core.models.models import WorkPacket

# =============================================================================
# TEST CONFIGURATION
# =============================================================================

TEST_DATASOURCE_ID: str = "ds_local_test_files"  # Options: "ds_local_test_files", "ds_smb_finance_dept", "ds_sql_localhost"

GROUND_TRUTH_CONFIG = {
    "ds_sql_localhost": {
        "type": "sql",
        "connection_string": "mssql+pyodbc://localhost/MyTest?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    },
    "ds_local_test_files": {
        "type": "file",
        "path": "C:/TestDataClassification"
    },
    "ds_smb_finance_dept": {
        "type": "file",
        "path": "C:/TestDataClassification/test_files"
    }
}

#CLASSIFIER_TEMPLATE_ID: str = "general_corporate_v1.0"
CLASSIFIER_TEMPLATE_ID: str = "unstructured"
CONFIG_FILE_PATH: str = "config/system_default.yaml"
MOCK_JOB_ID: int = 999

# =============================================================================
# GROUND TRUTH BUILDER
# =============================================================================

async def build_ground_truth(datasource_id: str, config: Dict) -> Set[str]:
    """Connects to the source to build a list of all expected object paths."""
    print(f"\n[GROUND TRUTH] Building for {datasource_id}...")
    expected_paths = set()
    source_type = config.get("type")

    if source_type == "sql":
        try:
            engine = create_engine(config["connection_string"])
            with engine.connect() as connection:
                query = text(
                    "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME "
                    "FROM INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_TYPE = 'BASE TABLE'"
                )
                result = connection.execute(query)
                for row in result:
                    path = f"{row[0]}.{row[1]}.{row[2]}"
                    expected_paths.add(path)
            print(f"[GROUND TRUTH] Found {len(expected_paths)} tables in database")
        except Exception as e:
            print(f"[GROUND TRUTH ERROR] Could not build ground truth for SQL: {e}")

    elif source_type == "file":
        source_path = config["path"]
        if not os.path.isdir(source_path):
            print(f"[GROUND TRUTH ERROR] Path does not exist: {source_path}")
            return set()
        
        for root, dirs, files in os.walk(source_path):
            relative_root = os.path.relpath(root, source_path)
            if relative_root == '.':
                relative_root = ''

            for name in files:
                path = os.path.join(relative_root, name).replace('\\', '/')
                expected_paths.add(path)
            for name in dirs:
                path = os.path.join(relative_root, name).replace('\\', '/')
                expected_paths.add(path)
        
        print(f"[GROUND TRUTH] Found {len(expected_paths)} files and directories")
        
    return expected_paths


# =============================================================================
# MAIN TEST FUNCTION
# =============================================================================

async def run_job_simulation():
    """
    Simulates a full end-to-end job lifecycle with live discovery and verification.
    Uses production Orchestrator components without starting background coroutines.
    """
    db_interface = None
    
    try:
        # =====================================================================
        # PHASE 0: INITIALIZATION
        # =====================================================================
        print("="*60)
        print("PHASE 0: INITIALIZATION")
        print("="*60)
        
        # Configure logging
        root_logger = logging.getLogger()
        if root_logger.handlers:
            for handler in root_logger.handlers:
                root_logger.removeHandler(handler)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(JsonFormatter())
        #console_handler.setFormatter()
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)
        
        # Initialize core components
        error_handler = ErrorHandler()
        config_manager = ConfigurationManager(CONFIG_FILE_PATH)
        mock_ambient_context = {
            "component_name": "E2ETest",
            "machine_name": "test_host",
            "node_group": "default",
            "deployment_model": "single_process"
        }
        logger = SystemLogger(logging.getLogger(__name__), "TEXT", mock_ambient_context)
        config_manager.set_core_services(logger, error_handler)
        
        # Initialize database
        db_interface = DatabaseInterface(
            config_manager.get_db_connection_string(),
            logger,
            error_handler
        )
        await db_interface.test_connection()
        await config_manager.load_database_overrides_async(db_interface)
        config_manager.finalize()
        settings = config_manager.settings
        db_interface.logger = logger
        
        # Initialize credential manager and connector factory
        credential_manager = CredentialManager()
        connector_factory = ConnectorFactory(
            logger=logger,
            error_handler=error_handler,
            db_interface=db_interface,
            credential_manager=credential_manager,
            system_config=settings
        )
        
        # Create orchestrator (WITHOUT starting coroutines)
        print("\n[INIT] Creating orchestrator instance...", flush=True)
        orchestrator = Orchestrator(
            settings=settings,
            db=db_interface,
            logger=logger,
            error_handler=error_handler
        )
        orchestrator._in_process_work_queue = asyncio.Queue()
        
        # Create worker
        print("[INIT] Creating worker instance...")
        worker = Worker(
            worker_id="e2e_worker_01",
            settings=settings,
            db_interface=db_interface,
            connector_factory=connector_factory,
            logger=logger,
            config_manager=config_manager,
            error_handler=error_handler,
            orchestrator_instance=orchestrator
        )
        
        # Setup test job
        await setup_test_job(MOCK_JOB_ID, TEST_DATASOURCE_ID, orchestrator, db_interface)
        
        # Build ground truth
        expected_paths = await build_ground_truth(
            TEST_DATASOURCE_ID,
            GROUND_TRUTH_CONFIG[TEST_DATASOURCE_ID]
        )
        
        if not expected_paths:
            print("[ERROR] Could not build ground truth. Aborting test.")
            return
        
        # =====================================================================
        # PHASE 1: MAIN ORCHESTRATION LOOP
        # =====================================================================
        print("\n" + "="*60, flush=True)
        print("PHASE 1: ORCHESTRATION LOOP", flush=True)
        print("="*60)
        
        max_iterations = 20
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            print(f"\n{'='*60}", flush=True)
            print(f"ITERATION {iteration}", flush=True)
            print(f"{'='*60}", flush=True)
            
            # STEP 1: Task Assignment
            print("\n[STEP 1: TASK ASSIGNMENT]")
            dispatched = 0
            while True:
                task = await orchestrator.task_assigner._find_and_approve_task()
                if not task:
                    break
                await orchestrator.task_assigner._dispatch_task(task)
                dispatched += 1
            
            print(f"  Dispatched: {dispatched} tasks to queue", flush=True)
            
            # STEP 2: Worker Processing
            print("\n[STEP 2: WORKER PROCESSING]")
            tasks_processed = 0
            while not orchestrator._in_process_work_queue.empty():
                work_packet_dict = await orchestrator.get_task_async(worker.worker_id)
                if work_packet_dict:
                    work_packet = WorkPacket(**work_packet_dict)
                    task_type = work_packet.payload.task_type
                    task_id_short = work_packet.header.task_id[:8]
                    print(f"  Processing: {task_type} | Task ID: {task_id_short}...", flush=True)
                    
                    await worker._process_task_async(work_packet)
                    tasks_processed += 1
            
            print(f"  Processed: {tasks_processed} tasks", flush=True)
            
            # STEP 3: Pipelining
            print("\n[STEP 3: PIPELINING]")
            tasks_created = await orchestrator.pipeliner.process_batch(
                job_id_filter=MOCK_JOB_ID,
                limit=100
            )
            print(f"  Created: {tasks_created} new tasks from output records")
            
            # EXIT CONDITION
            if dispatched == 0 and tasks_processed == 0 and tasks_created == 0:
                print("\n[NO MORE WORK DETECTED]")
                
                # Check if tasks are blocked vs truly complete
                result = await db_interface.execute_raw_sql(
                    f"SELECT COUNT(*) as cnt FROM tasks "
                    f"WHERE job_id = {MOCK_JOB_ID} AND status = 'PENDING'"
                )
                pending_count = result[0]['cnt'] if result else 0
                
                if pending_count > 0:
                    print(f"  WARNING: {pending_count} tasks still PENDING", flush=True)
                    print("  (likely blocked by resource/schedule constraints)", flush=True)
                else:
                    print("  All tasks completed successfully", flush=True)
                
                break
        
        print(f"\n{'='*60}", flush=True)
        print(f"ORCHESTRATION COMPLETE - {iteration} iterations", flush=True)
        print(f"{'='*60}", flush=True)
        
        # =====================================================================
        # PHASE 2: VERIFICATION
        # =====================================================================
        print("\n" + "="*60, flush=True)
        print("PHASE 2: VERIFICATION", flush=True)
        print("="*60, flush=True)
        
        # Gather actual results
        actual_objects = await db_interface.get_objects_by_datasource(
            TEST_DATASOURCE_ID,
            limit=10000
        )
        actual_paths = {obj.object_path for obj in actual_objects}
        
        # Normalize paths for comparison
        def normalize(path: str) -> str:
            return path.replace('[', '').replace(']', '').replace('"', '').replace('`', '').lower()
        
        actual_normalized = {normalize(p) for p in actual_paths}
        expected_normalized = {normalize(p) for p in expected_paths}
        
        # Calculate differences
        matched = actual_normalized & expected_normalized
        missing = expected_normalized - actual_normalized
        unexpected = actual_normalized - expected_normalized
        
        # Metadata verification
        actual_hashes = [obj.object_key_hash for obj in actual_objects]
        metadata_count = 0
        if actual_hashes:
            async with db_interface.get_async_session() as session:
                stmt = select(func.count()).select_from(ObjectMetadata)
                result = await session.execute(stmt)
                metadata_count = result.scalar_one()

        findings_count = 0
        if actual_hashes:
            async with db_interface.get_async_session() as session:
                stmt = select(func.count()).select_from(ScanFindingSummary)
                result = await session.execute(stmt)
                findings_count = result.scalar_one()
        



        # Print results
        print(f"\n{'='*60}", flush=True)
        print("VERIFICATION RESULTS", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"Expected objects:    {len(expected_normalized)}", flush=True)
        print(f"Discovered objects:  {len(actual_normalized)}", flush=True)
        print(f"Matched:             {len(matched)}", flush=True)
        print(f"Missing:             {len(missing)}", flush=True)
        print(f"Unexpected:          {len(unexpected)}", flush=True)
        print(f"With metadata:       {metadata_count}", flush=True)
        print(f"Total Findings:      {findings_count}", flush=True)
        
        # Pass/Fail determination
        if not missing and not unexpected:
            print(f"\n{'='*60}", flush=True)
            print("RESULT: PASS", flush=True)
            print(f"{'='*60}")
        else:
            print(f"\n{'='*60}", flush=True)
            print("RESULT: FAIL", flush=True)
            print(f"{'='*60}", flush=True)
            
            if missing:
                print(f"\nMissing objects (showing up to 10):")
                for path in sorted(list(missing))[:10]:
                    print(f"  - {path}", flush=True)
                if len(missing) > 10:
                    print(f"  ... and {len(missing) - 10} more", flush=True)
            
            if unexpected:
                print(f"\nUnexpected objects (showing up to 10):", flush=True)
                for path in sorted(list(unexpected))[:10]:
                    print(f"  - {path}", flush=True)
                if len(unexpected) > 10:
                    print(f"  ... and {len(unexpected) - 10} more", flush=True)
    
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}", flush=True)
        logging.exception("An unhandled exception occurred during test execution")
    
    finally:
        if db_interface:
            await db_interface.close_async()
        print("\n" + "="*60, flush=True)
        print("TEST SCRIPT COMPLETE", flush=True)
        print("="*60, flush=True)


async def setup_test_job(job_id: int, datasource_id: str, orchestrator, db: DatabaseInterface):
    from core.db_models.job_schema import Job, JobType, JobStatus
    
    print("\n[SETUP: Cleaning old test data]")
    await db.execute_raw_sql(f"DELETE FROM discovered_objects ")
    await db.execute_raw_sql(f"DELETE FROM object_metadata ")
    await db.execute_raw_sql(f"DELETE FROM scan_finding_summaries ")    
    await db.execute_raw_sql(f"DELETE FROM task_output_records WHERE job_id = {job_id}")
    await db.execute_raw_sql(f"DELETE FROM tasks WHERE job_id = {job_id}")
    await db.execute_raw_sql(f"DELETE FROM jobs WHERE id = {job_id}")
    
    print("[SETUP: Creating job record]")
    async with db.get_async_session() as session:
        job = Job(
            id=job_id,
            execution_id=f"e2e_test_{job_id}",
            template_table_id=1,  # CORRECTED: Added missing required field
            template_type=JobType.SCANNING,
            status=JobStatus.QUEUED,
            trigger_type='e2e_test',  # CORRECTED: Added missing required field
            #orchestrator_id=orchestrator.instance_id,
            node_group=orchestrator.settings.system.node_group,
            #orchestrator_lease_expiry=datetime.now(timezone.utc) + timedelta(hours=2),  # CORRECTED: Renamed field
            configuration={
                'datasource_targets': [{'datasource_id': datasource_id}],
                'classifier_template_id': CLASSIFIER_TEMPLATE_ID,
                'discovery_workflow': 'two-phase',
                'discovery_mode': 'FULL'
            },
            priority=1,
            created_timestamp=datetime.now(timezone.utc),
            version=1
        )
        session.add(job)
        await session.commit()

    
    print("[SETUP: Registering job in orchestrator state]")
    await orchestrator._update_job_in_memory_state(
        job_id=job_id,
        new_status=JobState.QUEUED,
        version=1,
        lease_expiry=datetime.now(timezone.utc) + timedelta(hours=2),
        is_new=True
    )
    
    print("[SETUP: Claiming job and creating initial task]")
    success = await orchestrator.task_assigner._claim_and_initialize_new_job()
    if not success:
        raise RuntimeError("Failed to claim and initialize job")
    
    print("[SETUP: Complete]\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    asyncio.run(run_job_simulation())