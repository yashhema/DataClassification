# src/main.py
"""
The main entry point for the application.

This script acts as a flexible launcher, capable of starting the system in
different modes based on command-line arguments. This version incorporates
robust logging, error handling, and lifecycle management, including the
capture of ambient system context for distributed traceability.

INTEGRATION STATUS: Tasks 5 & 11 Complete
- Real component imports and dependency injection
- Full async architecture with proper event loop management
- Support for single-process, orchestrator, and worker deployment modes
"""

import argparse
import asyncio
import logging
import signal
import sys
import os
import socket
from uuid import uuid4
from typing import Optional, List

# Core infrastructure imports
from core.config.configuration_manager import ConfigurationManager  
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler, ConfigurationError
from core.logging.system_logger import SystemLogger

# Real component imports (replacing placeholder classes)
from orchestrator.orchestrator import Orchestrator
from worker.worker import Worker, ConnectorFactory
from content_extraction.content_extractor import ContentExtractor
from models import ContentExtractionConfig
from connectors.smb_connector import SMBConnector
from connectors.sql_server_connector import SQLServerConnector

# Global shutdown event for graceful async shutdown
shutdown_event = asyncio.Event()

def graceful_shutdown_handler(signum, frame):
    """Signal handler to initiate a graceful async shutdown."""
    print(f"\nShutdown signal {signum} received. Initiating graceful shutdown...")
    # Set the event in a thread-safe way
    asyncio.create_task(shutdown_event.set()) if asyncio.get_running_loop() else shutdown_event.set()

async def create_system_components(settings, logger, error_handler):
    """
    Create and configure all system components with proper dependency injection.
    
    Returns:
        tuple: (orchestrator, workers, db_interface, content_extractor, connector_factory)
    """
    startup_trace_id = f"component_init_{uuid4().hex}"
    base_log_context = {"trace_id": startup_trace_id}
    
    try:
        # 1. Initialize async database interface
        logger.info("Initializing async database interface...", **base_log_context)
        db_interface = DatabaseInterface(settings.database.connection_string, logger, error_handler)
        
        # Test database connectivity
        await db_interface.test_connection()
        logger.info("Database connection established successfully", **base_log_context)
        
        # 2. Initialize content extraction system
        logger.info("Initializing content extraction system...", **base_log_context)
        extraction_config = ContentExtractionConfig.from_dict(settings.content_extraction.dict())
        content_extractor = ContentExtractor(extraction_config, settings, logger, error_handler)
        logger.info("Content extractor initialized successfully", **base_log_context)
        
        # 3. Initialize connector factory and register connectors
        logger.info("Initializing connector factory...", **base_log_context)
        connector_factory = ConnectorFactory(logger, error_handler)
        
        # Register SMB connector
        if hasattr(settings, 'smb_datasources'):
            for smb_config in settings.smb_datasources:
                smb_connector = SMBConnector(
                    datasource_id=smb_config.id,
                    smb_config=smb_config,
                    content_extractor=content_extractor,
                    logger=logger,
                    error_handler=error_handler
                )
                connector_factory.register_connector("SMB", smb_connector)
                logger.info(f"SMB connector registered for datasource: {smb_config.id}", **base_log_context)
        
        # Register SQL Server connector
        if hasattr(settings, 'database_datasources'):
            for db_config in settings.database_datasources:
                sql_connector = SQLServerConnector(
                    datasource_id=db_config.id,
                    connection=db_interface,
                    logger=logger,
                    error_handler=error_handler
                )
                connector_factory.register_connector("DATABASE", sql_connector)
                logger.info(f"SQL Server connector registered for datasource: {db_config.id}", **base_log_context)
        
        # 4. Initialize orchestrator (always needed for job management)
        logger.info("Initializing orchestrator...", **base_log_context)
        orchestrator = Orchestrator(settings, db_interface, logger, error_handler)
        
        # Test orchestrator health
        if not await orchestrator.is_healthy():
            raise SystemExit("Orchestrator health check failed during initialization")
        
        logger.info("Orchestrator initialized successfully", **base_log_context)
        
        # 5. Initialize workers if needed
        workers = []
        if settings.orchestrator.deployment_model.lower() in ["worker", "single_process"]:
            logger.info("Initializing workers...", **base_log_context)
            
            # Create workers based on configuration
            worker_count = getattr(settings.worker, 'instance_count', 1)
            for i in range(worker_count):
                worker = Worker(
                    worker_id=f"worker_{i}",
                    settings=settings,
                    db_interface=db_interface,
                    connector_factory=connector_factory,
                    logger=logger,
                    error_handler=error_handler
                )
                
                # Test worker health
                if not await worker.is_healthy():
                    raise SystemExit(f"Worker {i} health check failed during initialization")
                
                workers.append(worker)
                logger.info(f"Worker {i} initialized successfully", **base_log_context)
        
        logger.log_component_lifecycle("System", "COMPONENT_INITIALIZATION_SUCCESS", **base_log_context)
        return orchestrator, workers, db_interface, content_extractor, connector_factory
        
    except Exception as e:
        error_handler.handle_error(e, logger, **base_log_context, operation_phase="component_initialization")
        raise

async def run_single_process_mode(orchestrator: Orchestrator, workers: List[Worker], logger, base_log_context):
    """
    Run the system in single-process mode with all components in one async event loop.
    """
    logger.info("Starting single-process mode...", **base_log_context)
    
    # Create async tasks for all components
    tasks = []
    
    # Start orchestrator (includes API and background threads as coroutines)
    tasks.append(asyncio.create_task(orchestrator.start_async(), name="orchestrator"))
    
    # Start all workers
    for i, worker in enumerate(workers):
        tasks.append(asyncio.create_task(worker.start_async(), name=f"worker_{i}"))
    
    logger.info(f"Single-process mode started with {len(tasks)} async tasks", **base_log_context)
    
    # Wait for shutdown signal
    await shutdown_event.wait()
    
    # Graceful shutdown
    logger.info("Shutting down single-process mode...", **base_log_context)
    
    # Cancel all tasks
    for task in tasks:
        task.cancel()
    
    # Wait for all tasks to complete with timeout
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("Some tasks did not shutdown gracefully within timeout", **base_log_context)
    
    # Shutdown components
    await orchestrator.shutdown_async()
    for worker in workers:
        await worker.shutdown_async()
    
    logger.info("Single-process mode shutdown complete", **base_log_context)

async def run_orchestrator_mode(orchestrator: Orchestrator, logger, base_log_context):
    """
    Run the system in orchestrator-only mode.
    """
    logger.info("Starting orchestrator-only mode...", **base_log_context)
    
    # Start orchestrator with API and background coroutines
    orchestrator_task = asyncio.create_task(orchestrator.start_async(), name="orchestrator")
    
    logger.info("Orchestrator-only mode started", **base_log_context)
    
    # Wait for shutdown signal
    await shutdown_event.wait()
    
    # Graceful shutdown
    logger.info("Shutting down orchestrator-only mode...", **base_log_context)
    
    orchestrator_task.cancel()
    
    try:
        await asyncio.wait_for(orchestrator_task, timeout=30.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        logger.warning("Orchestrator did not shutdown gracefully within timeout", **base_log_context)
    
    await orchestrator.shutdown_async()
    logger.info("Orchestrator-only mode shutdown complete", **base_log_context)

async def run_worker_mode(workers: List[Worker], logger, base_log_context):
    """
    Run the system in worker-only mode.
    """
    logger.info("Starting worker-only mode...", **base_log_context)
    
    # Create tasks for all workers
    worker_tasks = []
    for i, worker in enumerate(workers):
        task = asyncio.create_task(worker.start_async(), name=f"worker_{i}")
        worker_tasks.append(task)
    
    logger.info(f"Worker-only mode started with {len(worker_tasks)} workers", **base_log_context)
    
    # Wait for shutdown signal
    await shutdown_event.wait()
    
    # Graceful shutdown
    logger.info("Shutting down worker-only mode...", **base_log_context)
    
    # Cancel all worker tasks
    for task in worker_tasks:
        task.cancel()
    
    try:
        await asyncio.wait_for(asyncio.gather(*worker_tasks, return_exceptions=True), timeout=30.0)
    except asyncio.TimeoutError:
        logger.warning("Some workers did not shutdown gracefully within timeout", **base_log_context)
    
    # Shutdown all workers
    for worker in workers:
        await worker.shutdown_async()
    
    logger.info("Worker-only mode shutdown complete", **base_log_context)

async def main():
    """
    Async main application entry point.
    
    Handles configuration loading, component initialization, and mode-specific
    async execution with proper event loop management.
    """
    # --- 1. Argument Parsing & Initial Mode Selection ---
    parser = argparse.ArgumentParser(description="Main launcher for the classification system.")
    parser.add_argument("--mode", type=str, choices=["orchestrator", "worker", "single_process"], 
                       help="The mode to run the application in.")
    args = parser.parse_args()

    # --- 2. Configuration Loading & Validation ---
    try:
        # Phase 1: Load base configuration
        config_manager = ConfigurationManager("config/system_default.yaml")
        
        # Phase 2: Create logging infrastructure with partial context
        partial_context = config_manager.get_partial_ambient_context() 
        partial_context["component_name"] = f"Main-{os.getpid()}"
        
        system_logger = SystemLogger(logging.getLogger(__name__), "TEXT", partial_context)
        error_handler = ErrorHandler()
        
        # Phase 3: Inject services and complete configuration
        config_manager.set_core_services(system_logger, error_handler)
        db_interface = DatabaseInterface(config_manager.get_db_connection_string(), system_logger, error_handler)
        await config_manager.load_database_overrides_async(db_interface)
        config_manager.finalize()
        settings = config_manager.settings
        
    except Exception as e:
        print(f"FATAL: Failed to load configuration. Error: {e}", file=sys.stderr)
        sys.exit(1)

    # --- 3. Determine Final Run Mode & Gather Ambient Context ---
    run_mode = args.mode or getattr(settings.orchestrator, 'deployment_model', 'single_process').lower()
    
    # Gather the ambient context for logging and error reporting
    ambient_context = {
        "component_name": run_mode.capitalize(),
        "machine_name": os.environ.get("POD_NAME") or socket.gethostname(),
        "node_group": getattr(settings.system, 'node_group', 'default'),
        "deployment_model": run_mode
    }

    # --- 4. Initialize Core Services with Ambient Context ---
    logging.basicConfig(
        level=getattr(settings.logging, 'level', 'INFO').upper(), 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = SystemLogger(
        logging.getLogger(__name__), 
        log_format=getattr(settings.logging, 'format', 'TEXT'), 
        ambient_context=ambient_context
    )
    error_handler = ErrorHandler()
    
    startup_trace_id = f"startup_{uuid4().hex}"
    base_log_context = {"trace_id": startup_trace_id}
    logger.info(f"System starting up in {run_mode.upper()} mode.", **base_log_context)

    # --- 5. Component Initialization & Health Checks ---
    try:
        orchestrator, workers, db_interface, content_extractor, connector_factory = await create_system_components(
            settings, logger, error_handler
        )
        logger.log_component_lifecycle("System", "INITIALIZATION_SUCCESS", **base_log_context)

    except Exception as e:
        error_handler.handle_error(e, logger, **base_log_context, operation_phase="startup")
        sys.exit(1)

    # --- 6. Setup Signal Handlers for Graceful Async Shutdown ---
    signal.signal(signal.SIGINT, graceful_shutdown_handler)
    signal.signal(signal.SIGTERM, graceful_shutdown_handler)

    # --- 7. Mode-Specific Async Startup ---
    try:
        if run_mode == "single_process":
            await run_single_process_mode(orchestrator, workers, logger, base_log_context)
        elif run_mode == "orchestrator":
            await run_orchestrator_mode(orchestrator, logger, base_log_context)
        elif run_mode == "worker":
            await run_worker_mode(workers, logger, base_log_context)
        else:
            logger.error(f"Unknown run mode: {run_mode}", **base_log_context)
            sys.exit(1)
            
    except Exception as e:
        error_handler.handle_error(e, logger, **base_log_context, operation_phase="runtime")
        sys.exit(1)
    finally:
        # Final cleanup
        await db_interface.close_async() if hasattr(db_interface, 'close_async') else None
        logger.info("System shutdown complete.", trace_id=f"shutdown_{uuid4().hex}")

if __name__ == "__main__":
    # Task 11: Event Loop Integration - Use asyncio.run() for proper async entry point
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt. Shutdown complete.")
    except Exception as e:
        print(f"FATAL: Unhandled exception in main: {e}", file=sys.stderr)
        sys.exit(1)