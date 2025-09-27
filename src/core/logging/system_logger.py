# src/core/logging/system_logger.py
"""
Provides a final, production-ready, and context-aware logging utility
for the entire system, incorporating feedback on validation, performance,
and error handling integration for enterprise deployment.

FIXES APPLIED:
- Added thread-safe sampling counters with proper locking
- Added periodic cleanup mechanism for sampling counters to prevent memory leaks
- Maintained all existing logging functionality and method signatures
"""

import logging
import json
import random
import threading
from typing import Dict, Any, Optional, Tuple
from uuid import uuid4
from datetime import datetime, timedelta, timezone
import copy

# Assuming errors.py is in a sibling directory
from ..errors import ClassificationError

def custom_json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, (datetime, timedelta)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class JsonFormatter(logging.Formatter):
    """
    Custom formatter to output log records as a single JSON string.
    This is essential for log aggregation in distributed systems like EKS.
    """
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, 'extra_context'):
            log_record.update(record.extra_context)

        # ⬇️ STEP 2: Modify this line to use the new serializer ⬇️
        return json.dumps(log_record, default=custom_json_serializer)





class SystemLogger:
    """
    A structured logger that provides a comprehensive set of methods for
    logging specific system events. It includes context validation, performance
    guards, and support for structured (JSON) output with thread-safe sampling.
    """

    def __init__(self, logger: logging.Logger, log_format: str = "TEXT", ambient_context: Optional[Dict[str, Any]] = None):
        """
        Initializes the logger with a base logger instance and ambient context.

        - logger: The base Python logger instance.
        - log_format: The format for log output ('TEXT' or 'JSON').
        - ambient_context: A dictionary of context that is always present,
          e.g., {"component_name": "Orchestrator", "machine_name": "pod-123"}
        """
        self.logger = logger
        self.ambient_context = ambient_context or {}
        
        # Validate that all required ambient context fields are present.
        required_fields = ['deployment_model', 'component_name', 'machine_name', 'node_group']
        for field in required_fields:
            if field not in self.ambient_context:
                self.ambient_context[field] = 'UNKNOWN'
                # Use a basic log call here since the logger isn't fully configured yet.
                self.logger.warning(f"Ambient context is missing required field '{field}'. Defaulting to 'UNKNOWN'.")

        if log_format.upper() == "JSON":
            # Replace the default handler's formatter with our JSON formatter
            for handler in self.logger.handlers:
                handler.setFormatter(JsonFormatter())

        # FIXED: Thread-safe sampling counters with periodic cleanup
        self._sampling_lock = threading.Lock()
        self._sampling_counters: Dict[str, Tuple[int, datetime]] = {}  # For tracking dropped log messages
        self._last_cleanup_time = datetime.now(timezone.utc)
        self._cleanup_interval_seconds = 300  # Clean up every 5 minutes
        self._max_counter_age_seconds = 1800  # Remove counters older than 30 minutes

    def _log(self, level: int, message: str, context: Dict[str, Any]):
        """
        Internal log method that merges ambient and specific context
        and emits the final log record.
        """
        final_context = {**self.ambient_context, **context}
        self.logger.log(level, message, extra={"extra_context": final_context})

    def _ensure_trace_id(self, trace_id: Optional[str], context: Dict[str, Any]) -> str:
        """Validates trace_id, creating one if missing, and returns it."""
        if not trace_id:
            trace_id = f"missing_trace_{uuid4().hex}"
            context['trace_warning'] = 'trace_id was missing or empty'
        return trace_id

    def _cleanup_sampling_counters_if_needed(self):
        """Periodically clean up old sampling counters to prevent memory leaks."""
        now = datetime.now(timezone.utc)
        
        # Check if cleanup is needed (time-based)
        if (now - self._last_cleanup_time).total_seconds() < self._cleanup_interval_seconds:
            return
            
        with self._sampling_lock:
            # Remove counters older than the max age
            cutoff_time = now - timedelta(seconds=self._max_counter_age_seconds)
            counters_to_remove = []
            
            for counter_key, (count, last_logged) in self._sampling_counters.items():
                if last_logged < cutoff_time:
                    counters_to_remove.append(counter_key)
            
            for counter_key in counters_to_remove:
                del self._sampling_counters[counter_key]
            
            # Update last cleanup time
            self._last_cleanup_time = now
            
            if counters_to_remove:
                self.logger.debug(f"Cleaned up {len(counters_to_remove)} old sampling counters")

    # --- Configuration & Startup Logging ---

    def log_config_load(self, status: str, **context):
        """Logs the status of configuration loading."""
        self._log(logging.INFO, f"Configuration loading {status}.", context)

    def log_component_lifecycle(self, component: str, event: str, **context):
        """Logs a lifecycle event for a major component (e.g., startup, shutdown)."""
        self._log(logging.INFO, f"Component '{component}' event: {event}.", context)

    def log_health_check(self, component: str, is_healthy: bool, **context):
        """Logs the result of a component health check."""
        level = logging.INFO if is_healthy else logging.ERROR
        status = "succeeded" if is_healthy else "failed"
        self._log(level, f"Health check for component '{component}' {status}.", context)

    # --- Task Lifecycle & Health Logging ---

    def log_task_assignment(self, task_id: int, worker_id: str, trace_id: Optional[str], **context):
        """Logs the assignment of a task to a worker with consistent trace_id handling."""
        final_trace_id = self._ensure_trace_id(trace_id, context)
        log_context = {"task_id": task_id, "worker_id": worker_id, "trace_id": final_trace_id, **context}
        self._log(logging.INFO, f"Task {task_id} assigned to worker {worker_id}.", log_context)

    def log_progress_batch(self, task_output_payload: Dict[str, Any], task_id: int, sampling_rate: float = 0.1, force_log: bool = False, **context):
        """Logs task progress with thread-safe sampling and automatic cleanup."""
        
        # Periodic cleanup of old sampling counters
        self._cleanup_sampling_counters_if_needed()
        
        if not force_log and random.random() > sampling_rate:
            counter_key = f"progress_batch_{task_id}"
            now = datetime.now(timezone.utc)
            
            # FIXED: Thread-safe sampling counter access
            with self._sampling_lock:
                count, last_logged = self._sampling_counters.get(counter_key, (0, now))
                new_count = count + 1
                self._sampling_counters[counter_key] = (new_count, last_logged)
                
                # Log suppression message periodically
                should_log_suppression = (
                    new_count % 1000 == 0 or  # Every 1000 messages
                    (now - last_logged).total_seconds() > 60  # Or every minute
                )
                
                if should_log_suppression:
                    self.info(f"Log sampling is active for task {task_id}. Suppressed {new_count} progress messages so far.", task_id=task_id)
                    # Update the last_logged time
                    self._sampling_counters[counter_key] = (new_count, now)
            return

        log_context = {
            "task_id": task_id, 
            "batch_id": task_output_payload.get("batch_id", "N/A"), 
            "item_count": task_output_payload.get("count", 0), 
            **context
        }
        self._log(logging.INFO, f"Task {task_id} reported progress batch.", log_context)

    def log_task_completion(self, task_id: int, status: str, **context):
        """Logs task completion and cleans up associated sampling counters."""
        level = logging.INFO if status.upper() == "COMPLETED" else logging.WARNING
        self._log(level, f"Task {task_id} finished with status: {status}.", {"task_id": task_id, **context})
        
        # FIXED: Thread-safe cleanup of sampling counter for completed task
        counter_key = f"progress_batch_{task_id}"
        with self._sampling_lock:
            self._sampling_counters.pop(counter_key, None)

    def log_heartbeat(self, task_id: int, worker_id: str, **context):
        """Logs heartbeat from a worker."""
        self._log(logging.DEBUG, f"Heartbeat received for task {task_id} from worker {worker_id}.", {"task_id": task_id, "worker_id": worker_id, **context})

    def log_lease_expiry(self, task_id: int, worker_id: str, **context):
        """Logs when a task lease expires and needs re-queuing."""
        self._log(logging.WARNING, f"Lease expired for task {task_id} on worker {worker_id}. Re-queuing.", {"task_id": task_id, "worker_id": worker_id, **context})

    def log_worker_health(self, worker_id: str, is_healthy: bool, **context):
        """Logs worker health status."""
        level = logging.INFO if is_healthy else logging.WARNING
        status = "healthy" if is_healthy else "unhealthy"
        self._log(level, f"Worker {worker_id} reported status: {status}.", {"worker_id": worker_id, **context})

    # --- Resource & Database Logging ---

    def log_resource_decision(self, job_id: int, decision: str, is_approved: bool, **context):
        """Logs a decision made by the Resource Coordinator."""
        status = "Approved" if is_approved else "Denied"
        level = logging.DEBUG if is_approved else logging.INFO
        self._log(level, f"Resource decision for job {job_id}: {decision} -> {status}", {"job_id": job_id, **context})

    def log_database_operation(self, main_operation: str, table: str, main_status: str, **context):
        # The log message would also need a small update to use the new name
        self._log(logging.DEBUG, f"Database operation '{main_operation}' on table '{table}' {main_status}.", context)
        # --- Error Handling Integration ---

    def log_classification_error(self, error: "ClassificationError", trace_id: Optional[str], **context):
        """Logs a structured error, correlating it with the current trace_id."""
        final_trace_id = self._ensure_trace_id(trace_id, context)
        
        # Create a copy to avoid mutating the original error object's context
        error_dict = copy.deepcopy(error.to_dict())
        error_dict['context']['trace_id'] = final_trace_id
        error_dict['context'].update(context)
        
        message = f"ClassificationError occurred: {error.message}"
        
        severity_map = {
            "CRITICAL": logging.CRITICAL,
            "HIGH": logging.ERROR,
            "MEDIUM": logging.WARNING,
            "LOW": logging.INFO
        }
        level = severity_map.get(error.severity.value.upper(), logging.ERROR)
        self._log(level, message, error_dict)

    # --- General-Purpose Logging ---

    def info(self, message: str, **context):
        """Log an info message."""
        self._log(logging.INFO, message, context)

    def warning(self, message: str, **context):
        """Log a warning message."""
        self._log(logging.WARNING, message, context)

    def error(self, message: str, **context):
        """Log an error message."""
        self._log(logging.ERROR, message, context)

    def debug(self, message: str, **context):
        """Log a debug message."""
        self._log(logging.DEBUG, message, context)
    def critical(self, message: str, **context):
        """Log a critical message."""
        self._log(logging.CRITICAL, message, context)
    # --- Utility Methods for System Health ---

    def get_sampling_stats(self) -> Dict[str, Any]:
        """Get current sampling counter statistics for monitoring."""
        with self._sampling_lock:
            stats = {
                "active_counters": len(self._sampling_counters),
                "last_cleanup": self._last_cleanup_time.isoformat(),
                "counters": {
                    key: {"count": count, "last_logged": last_logged.isoformat()}
                    for key, (count, last_logged) in self._sampling_counters.items()
                }
            }
        return stats

    def force_cleanup_sampling_counters(self):
        """Force immediate cleanup of all sampling counters (for testing/maintenance)."""
        with self._sampling_lock:
            cleared_count = len(self._sampling_counters)
            self._sampling_counters.clear()
            self._last_cleanup_time = datetime.now(timezone.utc)
        
        self.logger.info(f"Force cleaned {cleared_count} sampling counters")
        return cleared_count