# src/main.py
"""
The main entry point for the application service. This script acts as a flexible
launcher for starting the system in different modes.
"""

import argparse
import asyncio
import logging
import signal
import sys
import os
import sqlalchemy
from datetime import datetime, timezone, timedelta
# --- ADD THESE THREE LINES ---
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# -----------------------------
import socket
import queue
import traceback
from uuid import uuid4
from typing import List

# Core infrastructure imports
from core.config.configuration_manager import ConfigurationManager  
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger, JsonFormatter
from core.credentials.credential_manager import CredentialManager

# Real component imports
from orchestrator.orchestrator import Orchestrator
from worker.worker import Worker, ConnectorFactory
from content_extraction.content_extractor import ContentExtractor
from core.models.models import ContentExtractionConfig

# Global shutdown event
shutdown_event = asyncio.Event()
logger = None
def graceful_shutdown_handler(signum, frame):
    """Signal handler to initiate a graceful async shutdown."""
    print(f"\nShutdown signal {signum} received. Initiating graceful shutdown...")
    # CORRECTED: Simply call the set() method directly.
    # It is thread-safe and does not need to be wrapped in a task.
    shutdown_event.set()

async def create_system_components(settings, logger, error_handler, config_manager):
    """
    Create and configure all system components with proper dependency injection.
    """
    startup_trace_id = f"component_init_{uuid4().hex}"
    base_log_context = {"trace_id": startup_trace_id}
    db_interface = None
    
    try:
        # 1. Initialize async database interface
        logger.info("Initializing async database interface...", **base_log_context)
        db_interface = DatabaseInterface(settings.database.connection_string, logger, error_handler)
        await db_interface.test_connection()
        logger.info("Database connection established successfully", **base_log_context)
        
        # 2. Initialize content extraction system
        logger.info("Initializing content extraction system...", **base_log_context)
        extraction_config = ContentExtractionConfig.from_dict(settings.content_extraction.model_dump())
        content_extractor = ContentExtractor(extraction_config, settings, logger, error_handler)
        logger.info("Content extractor initialized successfully", **base_log_context)
        
        # 3. Initialize the Credential Manager
        logger.info("Initializing credential manager...", **base_log_context)
        credential_manager = CredentialManager()
        logger.info("Credential manager initialized successfully.", **base_log_context)

        # 4. Initialize the on-demand Connector Factory with all its dependencies
        logger.info("Initializing connector factory...", **base_log_context)
        connector_factory = ConnectorFactory(
            logger=logger,
            error_handler=error_handler,
            db_interface=db_interface,
            credential_manager=credential_manager,
            system_config=settings
        )
        
        # 5. Initialize orchestrator
        logger.info("Initializing orchestrator...", **base_log_context)
        orchestrator = Orchestrator(settings, db_interface, logger, error_handler)
        if not await orchestrator.is_healthy():
            raise SystemExit("Orchestrator health check failed during initialization")
        logger.info("Orchestrator initialized successfully", **base_log_context)
        
        # 6. Initialize workers if needed
        workers = []
        if settings.orchestrator.deployment_model.lower() in ["worker", "single_process"]:
            logger.info("Initializing workers...", **base_log_context)
            worker_count = getattr(settings.worker, 'instance_count', 1)
            for i in range(worker_count):
                worker = Worker(
                    worker_id=f"worker_{i}",
                    settings=settings,
                    db_interface=db_interface,
                    connector_factory=connector_factory,
                    config_manager=config_manager,
                    logger=logger,
                    error_handler=error_handler,
                    orchestrator_instance=orchestrator if settings.orchestrator.deployment_model.lower() == "single_process" else None
                )
                if not await worker.is_healthy():
                    raise SystemExit(f"Worker {i} health check failed during initialization")
                workers.append(worker)
                logger.info(f"Worker {i} initialized successfully", **base_log_context)
        
        logger.log_component_lifecycle("System", "COMPONENT_INITIALIZATION_SUCCESS", **base_log_context)
        return orchestrator, workers, db_interface
        
    except Exception as e:
        if isinstance(e, SystemExit):
            raise
        # Ensure error_handler is available to log the error
        if 'error_handler' not in locals():
            error_handler = ErrorHandler()
        if 'logger' not in locals():
            logger = SystemLogger(logging.getLogger(__name__), "TEXT", {})
            
        error_handler.handle_error(e, logger, **base_log_context, operation_phase="component_initialization")
        if db_interface:
            await db_interface.close_async()
        raise



async def run_single_process_mode(orchestrator: Orchestrator, workers: List[Worker], logger, base_log_context):
    logger.info("Starting single-process mode...", **base_log_context)
    tasks = []
    tasks.append(asyncio.create_task(orchestrator.start_async(asyncio.Queue()), name="orchestrator"))
    for i, worker in enumerate(workers):
        tasks.append(asyncio.create_task(worker.start_async(), name=f"worker_{i}"))
    
    logger.info(f"Single-process mode started with {len(tasks)} async tasks", **base_log_context)
    
    try:
        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=0.5)  # Increased to 500ms
                break
            except asyncio.TimeoutError:
                continue
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received", **base_log_context)
        shutdown_event.set()
    
    logger.info("Shutting down single-process mode...", **base_log_context)
    
    for task in tasks:
        task.cancel()
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("Some tasks did not shutdown gracefully within timeout", **base_log_context)
    
    await orchestrator.shutdown_async()
    for worker in workers:
        await worker.shutdown_async()
    logger.info("Single-process mode shutdown complete", **base_log_context)

async def run_orchestrator_mode(orchestrator: Orchestrator, logger, base_log_context):
    logger.info("Starting orchestrator-only mode...", **base_log_context)
    orchestrator_task = asyncio.create_task(orchestrator.start_async(queue.Queue()), name="orchestrator")
    logger.info("Orchestrator-only mode started", **base_log_context)
    await shutdown_event.wait()
    logger.info("Shutting down orchestrator-only mode...", **base_log_context)
    orchestrator_task.cancel()
    try:
        await asyncio.wait_for(orchestrator_task, timeout=30.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        logger.warning("Orchestrator did not shutdown gracefully within timeout", **base_log_context)
    await orchestrator.shutdown_async()
    logger.info("Orchestrator-only mode shutdown complete", **base_log_context)

async def run_worker_mode(workers: List[Worker], logger, base_log_context):
    logger.info("Starting worker-only mode...", **base_log_context)
    worker_tasks = [asyncio.create_task(w.start_async(), name=f"worker_{i}") for i, w in enumerate(workers)]
    logger.info(f"Worker-only mode started with {len(worker_tasks)} workers", **base_log_context)
    await shutdown_event.wait()
    logger.info("Shutting down worker-only mode...", **base_log_context)
    for task in worker_tasks:
        task.cancel()
    try:
        await asyncio.wait_for(asyncio.gather(*worker_tasks, return_exceptions=True), timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("Some workers did not shutdown gracefully within timeout", **base_log_context)
    for worker in workers:
        await worker.shutdown_async()
    logger.info("Worker-only mode shutdown complete", **base_log_context)
# Add this debugging task to your main() function, right after creating the CLI thread

async def debug_event_loop_activity():
    """Debug task that logs periodically to verify the event loop is active"""
    counter = 0
    # Add a check for the shutdown_event in the loop condition
    while not shutdown_event.is_set():
        try:
            # Instead of a simple sleep, wait for the event with a timeout.
            # This allows the loop to exit immediately when shutdown is triggered.
            await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            # This is the normal path; the timeout expires and we continue.
            counter += 1
            if logger: # Add a safety check for the logger
                logger.info(f"EVENT LOOP DEBUG: Tick #{counter} - Loop is processing tasks")

async def main():
    """ The main asynchronous entry point for the application service. """
    db_interface = None
    
    try:
        # 1. Argument Parsing & Initial Mode Selection
        parser = argparse.ArgumentParser(description="Main launcher for the classification system.")
        parser.add_argument("--mode", type=str, choices=["orchestrator", "worker", "single_process"], 
                           default="single_process", help="The mode to run the application in.")
        args = parser.parse_args()

        # 2. Configuration Loading (Phase 1: File Only)
        config_manager = ConfigurationManager("config/system_default.yaml")
        partial_context = config_manager.get_partial_ambient_context() 
        partial_context["component_name"] = f"Main-{os.getpid()}"
        # Initialize a temporary logger for startup
        temp_logger = SystemLogger(logging.getLogger("startup"), "TEXT", partial_context)
        error_handler = ErrorHandler()
        config_manager.set_core_services(temp_logger, error_handler)

        # 3. Initialize Database and Load Overrides (Phase 2: Inside async)
        db_interface = DatabaseInterface(config_manager.get_db_connection_string(), temp_logger, error_handler)
        
        # --- ADD THIS WARM-UP CALL ---
        # Force the connection pool to initialize before anything else uses it.
        await db_interface.test_connection()
        
        await config_manager.load_database_overrides_async(db_interface)
        config_manager.finalize()
        settings = config_manager.settings

        # 4. Determine Final Run Mode & Gather Ambient Context
        run_mode = args.mode or getattr(settings.orchestrator, 'deployment_model', 'single_process').lower()
        ambient_context = {
            "component_name": run_mode.capitalize(),
            "machine_name": os.environ.get("POD_NAME") or socket.gethostname(),
            "node_group": getattr(settings.system, 'node_group', 'default'),
            "deployment_model": run_mode
        }

        # 5. Initialize Final Logger & File Logging
        logging.basicConfig(level=settings.logging.level.upper(), format='%(asctime)s - %(levelname)s - %(message)s', force=True)

        if settings.logging.file and settings.logging.file.enabled:
            log_dir = os.path.dirname(settings.logging.file.path)
            if log_dir: os.makedirs(log_dir, exist_ok=True)

            # --- START OF CHANGE ---
            # 1. Generate a timestamp string
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # 2. Split the original path to insert the timestamp
            path_root, path_ext = os.path.splitext(settings.logging.file.path)
            
            # 3. Create the new, unique log file path
            unique_log_path = f"{path_root}_{timestamp}{path_ext}"
            
            # 4. Use the unique path to create the file handler
            file_handler = logging.FileHandler(unique_log_path)
            # --- END OF CHANGE ---
            file_handler.setLevel(settings.logging.file.level.upper())
            if settings.logging.file.format.upper() == "JSON":
                file_handler.setFormatter(JsonFormatter())
            else:
                file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logging.getLogger().addHandler(file_handler)
        
        logger = SystemLogger(logging.getLogger(__name__), log_format=settings.logging.format, ambient_context=ambient_context)
        # Re-assign the final logger to the db_interface
        db_interface.logger = logger
        
        startup_trace_id = f"startup_{uuid4().hex}"
        base_log_context = {"trace_id": startup_trace_id}
        logger.info(f"System starting up in {run_mode.upper()} mode.", **base_log_context)

        # 6. Component Initialization & Health Checks
        orchestrator, workers, db_interface_ref = await create_system_components(settings, logger, error_handler, config_manager)
        db_interface = db_interface_ref # Keep reference for the final close
        logger.log_component_lifecycle("System", "INITIALIZATION_SUCCESS", **base_log_context)

        # 7. Start CLI in background thread (ADD THIS NEW SECTION)
        logger.info("Starting CLI in background thread...", **base_log_context)
        
        import threading
        from cli import DatabaseInterfaceSyncWrapper, run_cli

        # Get main event loop reference BEFORE creating thread
        main_loop = asyncio.get_running_loop()
        logger.info(f"MAIN: Event loop ID: {id(main_loop)}", **base_log_context)
        
        settings = config_manager.settings
        def cli_wrapper():
            try:
                db_sync = DatabaseInterfaceSyncWrapper(db_interface, main_loop)
                run_cli(db_sync, settings)
            except Exception as e:
                logger.error(f"CLI thread error: {e}", **base_log_context)
                traceback.print_exc()

        cli_thread = threading.Thread(target=cli_wrapper, daemon=True)
        cli_thread.start()
        logger.info("CLI thread started", **base_log_context)

        # ADD THIS DEBUG TASK
        debug_task = asyncio.create_task(debug_event_loop_activity(), name="debug_loop")
        logger.info("Debug task started", **base_log_context)

        # 8. Setup Signal Handlers
        signal.signal(signal.SIGINT, graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, graceful_shutdown_handler)

        # 9. Mode-Specific Async Startup
        if run_mode == "single_process":
            await run_single_process_mode(orchestrator, workers, logger, base_log_context)
        elif run_mode == "orchestrator":
            await run_orchestrator_mode(orchestrator, logger, base_log_context)
        elif run_mode == "worker":
            await run_worker_mode(workers, logger, base_log_context)
        
    except Exception as e:
        final_logger = logger if logger else SystemLogger(logging.getLogger(__name__), "TEXT", {})
        if isinstance(e, SystemExit):
            final_logger.warning("System exiting due to initialization failure.")
        else:
            final_logger.error(f"FATAL: Unhandled exception in main: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db_interface:
            await db_interface.close_async()
        if logger:
            logger.info("System shutdown complete.", trace_id=f"shutdown_{uuid4().hex}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication shutdown.")
    except Exception as e:
        print("="*80, file=sys.stderr)
        print("FATAL, UNHANDLED EXCEPTION REACHED TOP-LEVEL.", file=sys.stderr)
        print(f"Error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        print("="*80, file=sys.stderr)
        
        # Brief delay to ensure stderr flushes
        import time
        time.sleep(0.1)
        
        sys.exit(1)