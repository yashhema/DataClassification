# src/main.py
"""
The main entry point for the application. This script acts as a flexible
launcher for starting the system in different modes and integrates the
decoupled CLI for single_process deployments.
"""

import argparse
import asyncio
import logging
import signal
import sys
import os
import socket
import queue
import threading
import time
from uuid import uuid4
from typing import Optional, List,Dict
import traceback
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

# Decoupled CLI imports
from orchestrator.orchestrator_api_shim import OrchestratorApiShim
from cli import run_cli

# Global shutdown event for graceful async shutdown
shutdown_event = asyncio.Event()

def graceful_shutdown_handler(signum, frame):
    """Signal handler to initiate a graceful async shutdown."""
    print(f"\nShutdown signal {signum} received. Initiating graceful shutdown...")
    # Set the event in a thread-safe way
    if asyncio.get_running_loop().is_running():
        asyncio.create_task(shutdown_event.set())
    else:
        shutdown_event.set()

async def create_system_components(settings, logger, error_handler):
    """
    Create and configure all system components with proper dependency injection.
    """
    startup_trace_id = f"component_init_{uuid4().hex}"
    base_log_context = {"trace_id": startup_trace_id}
    
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
                    logger=logger,
                    error_handler=error_handler,
                    orchestrator_instance=orchestrator if settings.orchestrator.deployment_model.lower() == "single_process" else None
                )
                if not await worker.is_healthy():
                    raise SystemExit(f"Worker {i} health check failed during initialization")
                workers.append(worker)
                logger.info(f"Worker {i} initialized successfully", **base_log_context)
        
        logger.log_component_lifecycle("System", "COMPONENT_INITIALIZATION_SUCCESS", **base_log_context)
        return orchestrator, workers, db_interface, content_extractor, connector_factory
        
    except Exception as e:
        error_handler.handle_error(e, logger, **base_log_context, operation_phase="component_initialization")
        raise

# These run mode functions remain unchanged
async def run_single_process_mode(orchestrator: Orchestrator, workers: List[Worker], logger, base_log_context, command_queue: queue.Queue):
    logger.info("Starting single-process mode...", **base_log_context)
    tasks = []
    tasks.append(asyncio.create_task(orchestrator.start_async(command_queue), name="orchestrator"))
    for i, worker in enumerate(workers):
        tasks.append(asyncio.create_task(worker.start_async(), name=f"worker_{i}"))
    logger.info(f"Single-process mode started with {len(tasks)} async tasks", **base_log_context)
    await shutdown_event.wait()
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
    # The command queue is not used in this mode, so we pass an empty one
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


# This is the new startup block that handles the decoupled CLI
if __name__ == "__main__":
    
    # This context is shared between the main app thread and the CLI thread.
    # It allows the main thread to signal when the orchestrator is ready.
    shared_context = {
        "orchestrator_instance": None,
        "command_queue": queue.Queue(),
        "db_interface": None
    }

    # This function will run in a separate thread and contains the main app logic.
    def run_main_app():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        shared_context["event_loop"] = loop        
        async def main_wrapper():
            # --- This is your original main() function, refactored ---
            
            # 1. Argument Parsing & Initial Mode Selection
            parser = argparse.ArgumentParser(description="Main launcher for the classification system.")
            parser.add_argument("--mode", type=str, choices=["orchestrator", "worker", "single_process"], 
                               help="The mode to run the application in.")
            args = parser.parse_args()

            # 2. Configuration Loading & Validation
            try:
                config_manager = ConfigurationManager("config/system_default.yaml")
                partial_context = config_manager.get_partial_ambient_context() 
                partial_context["component_name"] = f"Main-{os.getpid()}"
                system_logger = SystemLogger(logging.getLogger(__name__), "TEXT", partial_context)
                error_handler = ErrorHandler()
                config_manager.set_core_services(system_logger, error_handler)
                db_interface = DatabaseInterface(config_manager.get_db_connection_string(), system_logger, error_handler)
                await config_manager.load_database_overrides_async(db_interface)
                config_manager.finalize()
                settings = config_manager.settings
                shared_context["db_interface"] = db_interface
            except Exception as e:
                print(f"FATAL: Failed to load configuration. Error: {e}", file=sys.stderr)
                sys.exit(1)

            # 3. Determine Final Run Mode & Gather Ambient Context
            run_mode = args.mode or getattr(settings.orchestrator, 'deployment_model', 'single_process').lower()
            ambient_context = {
                "component_name": run_mode.capitalize(),
                "machine_name": os.environ.get("POD_NAME") or socket.gethostname(),
                "node_group": getattr(settings.system, 'node_group', 'default'),
                "deployment_model": run_mode
            }

            # 4. Initialize Core Services with Ambient Context & File Logging
            logging.basicConfig(level=settings.logging.level.upper(), format='%(asctime)s - %(levelname)s - %(message)s')
            if settings.logging.file and settings.logging.file.enabled:
                log_dir = os.path.dirname(settings.logging.file.path)
                if log_dir: os.makedirs(log_dir, exist_ok=True)
                file_handler = logging.FileHandler(settings.logging.file.path)
                file_handler.setLevel(settings.logging.file.level.upper())
                if settings.logging.file.format.upper() == "JSON":
                    file_handler.setFormatter(JsonFormatter())
                else:
                    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
                logging.getLogger().addHandler(file_handler)

            logger = SystemLogger(logging.getLogger(__name__), log_format=settings.logging.format, ambient_context=ambient_context)
            error_handler = ErrorHandler()
            startup_trace_id = f"startup_{uuid4().hex}"
            base_log_context = {"trace_id": startup_trace_id}
            logger.info(f"System starting up in {run_mode.upper()} mode.", **base_log_context)

            # 5. Component Initialization & Health Checks
            try:
                orchestrator, workers, _, _, _ = await create_system_components(settings, logger, error_handler)
                logger.log_component_lifecycle("System", "INITIALIZATION_SUCCESS", **base_log_context)
                # CRITICAL: Signal to the main thread that the orchestrator is ready
                shared_context["orchestrator_instance"] = orchestrator
            except Exception as e:
                error_handler.handle_error(e, logger, **base_log_context, operation_phase="startup")
                sys.exit(1)



            # 7. Mode-Specific Async Startup
            try:
                if run_mode == "single_process":
                    await run_single_process_mode(orchestrator, workers, logger, base_log_context, shared_context["command_queue"])
                elif run_mode == "orchestrator":
                    await run_orchestrator_mode(orchestrator, logger, base_log_context)
                elif run_mode == "worker":
                    await run_worker_mode(workers, logger, base_log_context)
            except Exception as e:
                error_handler.handle_error(e, logger, **base_log_context, operation_phase="runtime")
                sys.exit(1)
            finally:
                if shared_context["db_interface"]:
                    await shared_context["db_interface"].close_async()
                logger.info("System shutdown complete.", trace_id=f"shutdown_{uuid4().hex}")

        # --- Main app thread execution starts here ---
        try:
            loop.run_until_complete(main_wrapper())
        except Exception as e:
            print(f"FATAL: Main application thread crashed: {e}", file=sys.stderr)
            traceback.print_exc()

    # --- Main execution thread starts here ---
    try:
        # 1. Start the main application in a background thread
        app_thread = threading.Thread(target=run_main_app, name="AppThread", daemon=True)
        app_thread.start()

        # 2. Wait for the application to finish starting up and create the orchestrator
        print("Starting application, please wait...")
        start_wait_time = time.time()
        while shared_context["orchestrator_instance"] is None or shared_context["event_loop"] is None:
            if not app_thread.is_alive():
                raise RuntimeError("Application failed to start. Check logs for details.")
            if time.time() - start_wait_time > 60:
                raise RuntimeError("Application startup timed out.")
            time.sleep(0.2)
        print("Application started successfully.")

        # --- FIX: Set signal handlers in the main thread AFTER the app thread starts ---
        # This is safe because the main thread is not blocked.
        signal.signal(signal.SIGINT, graceful_shutdown_handler)
        signal.signal(signal.SIGTERM, graceful_shutdown_handler)

        # 3. Create the API shim, passing it the correct event loop from the app thread
        api_shim = OrchestratorApiShim(
            shared_context["orchestrator_instance"], 
            shared_context["command_queue"],
            shared_context["event_loop"]  # Pass the loop from the correct thread
        )
        run_cli(api_shim)

    except KeyboardInterrupt:
        print("\nCLI exited. Application shutting down.")
    except Exception as e:
        print(f"FATAL: Unhandled exception in main: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)