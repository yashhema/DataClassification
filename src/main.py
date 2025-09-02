# src/main.py
"""
The main entry point for the application.

This script acts as a flexible launcher, capable of starting the system in
different modes based on command-line arguments. This version incorporates
robust logging, error handling, and lifecycle management, including the
capture of ambient system context for distributed traceability.
"""

import argparse
import logging
import signal
import sys
import time
import threading
import socket
import os
from uuid import uuid4

# Import the core components and utilities we have designed
from core.config.configuration_manager import ConfigurationManager  
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler, ConfigurationError
from core.logging.system_logger import SystemLogger

# --- Placeholder Classes (to be replaced by real implementations) ---
class Orchestrator:
    def __init__(self, settings, db_interface, logger):
        self._logger = logger
        self._settings = settings
        self._db_interface = db_interface
        self._shutdown_event = False
        self._logger.log_component_lifecycle("Orchestrator", "INITIALIZED")

    def is_healthy(self) -> bool:
        self._logger.log_health_check("Orchestrator", True)
        return True

    def start(self):
        self._logger.log_component_lifecycle("Orchestrator", "STARTED")
    
    def start_api(self):
        self._logger.log_component_lifecycle("OrchestratorAPI", "STARTED")

    def start_worker_threads(self):
        self._logger.log_component_lifecycle("InProcessWorkerPool", "STARTED", thread_count=self._settings.worker.in_process_thread_count)

    def shutdown(self):
        self._logger.log_component_lifecycle("Orchestrator", "SHUTTING_DOWN")
        self._shutdown_event = True

class Worker:
    def __init__(self, settings, db_interface, logger):
        self._logger = logger
        self._settings = settings
        self._db_interface = db_interface
        self._shutdown_event = False
        self._logger.log_component_lifecycle("Worker", "INITIALIZED")

    def is_healthy(self) -> bool:
        self._logger.log_health_check("Worker", True)
        return True

    def start(self):
        self._logger.log_component_lifecycle("Worker", "STARTED")
    
    def shutdown(self):
        self._logger.log_component_lifecycle("Worker", "SHUTTING_DOWN")
        self._shutdown_event = True

# Global shutdown flag
shutdown_event = threading.Event()

def graceful_shutdown(signum, frame):
    """Signal handler to initiate a graceful shutdown."""
    print("\nShutdown signal received. Initiating graceful shutdown...")
    shutdown_event.set()

def main():
    """Main application entry point."""
    # --- 1. Argument Parsing & Initial Mode Selection ---
    parser = argparse.ArgumentParser(description="Main launcher for the classification system.")
    parser.add_argument("--mode", type=str, choices=["orchestrator", "worker"], help="The mode to run the application in.")
    args = parser.parse_args()

    # --- 2. Configuration Loading & Validation ---
    try:
        # Phase 1: Load base configuration
        config_manager = ConfigurationManager("config/system_default.yaml")
        
        # Phase 2: Create logging infrastructure with partial context
        partial_context = config_manager.get_partial_ambient_context() 
        # run_mode not available yet, so use generic component name for now
        partial_context["component_name"] = f"Main-{os.getpid()}"
        
        system_logger = SystemLogger(logging.getLogger(__name__), "TEXT", partial_context)
        error_handler = ErrorHandler()
        
        # Phase 3: Inject services and complete configuration
        config_manager.set_core_services(system_logger, error_handler)
        db_interface = DatabaseInterface(config_manager.get_db_connection_string(), system_logger, error_handler)
        config_manager.load_database_overrides(db_interface)
        config_manager.finalize()
        settings = config_manager.settings
        
    except Exception as e:
        print(f"FATAL: Failed to load configuration. Error: {e}", file=sys.stderr)
        sys.exit(1)
    # --- 3. Determine Final Run Mode & Gather Ambient Context ---
    # Per specification: command-line flag takes precedence.
    # If no flag, use the value from the now-loaded settings.
    run_mode = args.mode or settings.orchestrator.deployment_model.lower()
    
    # Gather the ambient context for logging and error reporting
    ambient_context = {
        "component_name": run_mode.capitalize(),
        "machine_name": os.environ.get("POD_NAME") or socket.gethostname(), # K8s standard
        "node_group": settings.system.node_group,
        "deployment_model": settings.orchestrator.deployment_model
    }

    # --- 4. Initialize Core Services with Ambient Context ---
    logging.basicConfig(level=settings.logging.level.upper(), format='%(asctime)s - %(levelname)s - %(message)s')
    logger = SystemLogger(logging.getLogger(__name__), log_format=settings.logging.format, ambient_context=ambient_context)
    error_handler = ErrorHandler()
    
    startup_trace_id = f"startup_{uuid4().hex}"
    base_log_context = {"trace_id": startup_trace_id}
    logger.info(f"System starting up in {run_mode.upper()} mode.", **base_log_context)

    # --- 5. Component Initialization & Health Checks ---
    orchestrator = None
    worker = None
    try:
        if run_mode in ["orchestrator", "single_process"]:
            orchestrator = Orchestrator(settings, db_interface, logger)
            if not orchestrator.is_healthy():
                raise SystemExit("Orchestrator health check failed on startup.")
        
        if run_mode in ["worker", "single_process"]:
            worker = Worker(settings, db_interface, logger)
            if not worker.is_healthy():
                raise SystemExit("Worker health check failed on startup.")
        
        logger.log_component_lifecycle("System", "INITIALIZATION_SUCCESS", **base_log_context)

    except Exception as e:
        error_handler.handle_error(e, logger, **base_log_context, operation_phase="startup")
        sys.exit(1)

    # --- 6. Start Services Based on Mode ---
    if run_mode == "orchestrator":
        orchestrator.start()
        orchestrator.start_api()
    elif run_mode == "worker":
        worker.start()
    elif run_mode == "single_process":
        orchestrator.start()
        orchestrator.start_api()
        orchestrator.start_worker_threads()
    
    # --- 7. Main Loop & Graceful Shutdown ---
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    while not shutdown_event.is_set():
        time.sleep(1)

    logger.info("System shutting down.", trace_id=f"shutdown_{uuid4().hex}")
    if orchestrator:
        orchestrator.shutdown()
    if worker and run_mode == "worker":
        worker.shutdown()
    
    print("Shutdown complete.")

if __name__ == "__main__":
    main()
