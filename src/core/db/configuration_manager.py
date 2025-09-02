# src/core/config/configuration_manager.py
"""
Provides a robust, type-safe configuration management system.

This version is fully integrated with the system's core logging and error 
handling frameworks and contains the complete schema for all system components.
"""

import logging
import socket
from typing import List, Dict, Any, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError as PydanticValidationError

from ..db_models.system_parameters_schema import SystemParameter
from ..errors import ConfigurationError

# Forward reference for core components to avoid circular imports at runtime
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..db.database_interface import DatabaseInterface
    from ..logging.system_logger import SystemLogger
    from ..errors import ErrorHandler

# =================================================================
# Pydantic Models for Type-Safe System Configuration
# =================================================================

class DatabaseConfig(BaseModel):
    connection_string: str

class RedisConfig(BaseModel):
    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0

class SystemIdentityConfig(BaseModel):
    component_name: str = Field("UNKNOWN")
    machine_name: str = Field(default_factory=socket.gethostname)
    node_group: str = Field("default")
    total_process_memory_limit_mb: int = Field(8192)
    local_timezone: str = Field("UTC", description="The local timezone for interpreting calendar schedules, e.g., 'America/New_York'.")

class WorkerConfig(BaseModel):
    in_process_thread_count: int = Field(8)
    task_timeout_seconds: int = Field(600)
    heartbeat_interval_seconds: int = Field(60)
    max_retries: int = Field(3)
    process_memory_limit_mb: int = Field(4096)
    global_cpu_threshold_percent: int = Field(90)

class OrchestratorConfig(BaseModel):
    deployment_model: str = Field("EKS")
    task_cache_size: int = Field(100)
    long_poll_timeout_seconds: int = Field(30)
    reaper_interval_seconds: int = Field(120)
    job_monitor_interval_sec: int = Field(30)
    pipeliner_interval_sec: int = Field(10)
    task_assigner_interval_sec: int = Field(2)
    cli_enabled: bool = Field(True, description="Enable the interactive CLI for job control during development.")

class JobConfig(BaseModel):
    failure_threshold_percent: int = Field(10)

class ConnectorConfig(BaseModel):
    discovery_batch_write_size: int = Field(1000)
    easyocr_support_enabled: bool = Field(True)
    get_table_or_images_enabled: bool = Field(True)

class LoggingConfig(BaseModel):
    format: str = Field("TEXT")
    level: str = Field("INFO")
    progress_sampling_rate: float = Field(0.1)

class ErrorHandlingConfig(BaseModel):
    persistence_enabled: bool = Field(True)
    circuit_breaker_threshold: int = Field(5)

class ClassificationConfig(BaseModel):
    """Expanded configuration for the classification subsystem."""
    max_content_size_mb: int = Field(100)
    default_row_limit: int = Field(1000)
    max_samples_per_finding: int = Field(100, description="Max sample occurrences to store in the DB for each summary.")
    confidence_scoring: ClassificationConfidenceConfig = Field(default_factory=ClassificationConfidenceConfig)

class TaskCostEstimationConfig(BaseModel):
    classification_mb_per_gb_file: int = Field(128)
    default_task_cost_mb: int = Field(256)

class ClassificationConfidenceConfig(BaseModel):
    """Defines thresholds for aggregated confidence scoring."""
    low_threshold: int = Field(30, description="Upper bound for LOW confidence score (e.g., 0-30).")
    medium_threshold: int = Field(60, description="Upper bound for MEDIUM confidence score (e.g., 31-60).")
    # Anything above medium is considered HIGH

class SystemConfig(BaseModel):
    """The root model for the entire system configuration."""
    database: DatabaseConfig
    redis: RedisConfig
    system: SystemIdentityConfig
    worker: WorkerConfig
    orchestrator: OrchestratorConfig
    job: JobConfig
    connector: ConnectorConfig
    logging: LoggingConfig
    errors: ErrorHandlingConfig
    classification: ClassificationConfig = Field(default_factory=ClassificationConfig)
    task_cost_estimation: TaskCostEstimationConfig = Field(default_factory=TaskCostEstimationConfig)

# =================================================================
# The Configuration Manager
# =================================================================

class ConfigurationManager:
    """Manages application configuration using a three-phase initialization."""
    
    def __init__(self, default_config_path: str):
        """Phase 1: Initialize with no dependencies to load the base file."""
        self.logger: Optional["SystemLogger"] = None
        self.error_handler: Optional["ErrorHandler"] = None
        self._raw_config: Optional[Dict[str, Any]] = None
        self._change_report: List[str] = []
        self.settings: Optional[SystemConfig] = None

        try:
            with open(default_config_path, 'r') as f:
                self._raw_config = yaml.safe_load(f)
            if not isinstance(self._raw_config, dict):
                raise yaml.YAMLError("Root of configuration file is not a dictionary.")
        except (FileNotFoundError, yaml.YAMLError, Exception) as e:
            raise ValueError(f"CRITICAL: Failed to load base configuration file '{default_config_path}': {e}") from e

    def set_core_services(self, system_logger: "SystemLogger", error_handler: "ErrorHandler"):
        """Phase 2: Inject core services once they are initialized."""
        self.logger = system_logger
        self.error_handler = error_handler
        self.logger.info("Core services (Logger, ErrorHandler) have been injected into ConfigurationManager.")

    def _ensure_services(self):
        """Internal check to ensure core services have been injected."""
        if not self.logger or not self.error_handler:
            raise RuntimeError("ConfigurationManager cannot perform this action until set_core_services() is called.")

    def get_partial_ambient_context(self) -> Dict[str, Any]:
        """Gets essential context from the raw config for logger initialization."""
        system_section = self._raw_config.get("system", {})
        orchestrator_section = self._raw_config.get("orchestrator", {})
        return {
            "machine_name": system_section.get("machine_name", socket.gethostname()),
            "node_group": system_section.get("node_group", "default"),
            "deployment_model": orchestrator_section.get("deployment_model", "EKS"),
        }

    def load_database_overrides(self, db_interface: "DatabaseInterface", context: Optional[Dict[str, Any]] = None):
        """Phase 3: Load overrides using the now-injected services."""
        self._ensure_services()
        context = context or {}
        self.logger.info("Fetching system parameters from database...", **context)
        try:
            global_params = db_interface.get_system_parameters()
            self._merge_params(global_params)
            
            node_group = self._raw_config.get("system", {}).get("node_group")
            if node_group:
                node_params = db_interface.get_system_parameters(node_group=node_group)
                self._merge_params(node_params)
        except Exception as e:
            self.error_handler.handle_error(e, context="load_database_overrides", **context)
            raise

    def finalize(self, context: Optional[Dict[str, Any]] = None):
        """Phase 3: Finalize and validate the configuration."""
        self._ensure_services()
        context = context or {}
        self.logger.info("Validating and finalizing system configuration...", **context)
        try:
            self.settings = SystemConfig.parse_obj(self._raw_config)

            if self._change_report:
                self.logger.info("Configuration overrides applied:", **context)
                for change in self._change_report:
                    self.logger.info(f"  - {change}", **context)
            else:
                self.logger.info("No database values were overridden.", **context)

            self.logger.info("Configuration has been validated and finalized successfully. ✅", **context)
        except PydanticValidationError as e:
            self.error_handler.handle_error(e, context="finalize_config_validation", **context)
            raise

    def _merge_params(self, params: List[SystemParameter]):
        """Merges parameters, tracking and logging any changes."""
        for param in params:
            keys = param.parameter_name.split('.')
            d = self._raw_config
            for key in keys[:-1]:
                d = d.setdefault(key, {})
            
            target_key = keys[-1]
            old_value = d.get(target_key)
            new_value = param.parameter_value

            if old_value != new_value:
                change_msg = f"Parameter '{param.parameter_name}': '{old_value}' -> '{new_value}'"
                self._change_report.append(change_msg)
                d[target_key] = new_value

    def get_db_connection_string(self) -> str:
        if not self._raw_config or 'database' not in self._raw_config or 'connection_string' not in self._raw_config['database']:
            raise ValueError("Database connection string is missing from the base configuration.")
        return self._raw_config['database']['connection_string']

    def is_healthy(self) -> bool:
        return self.settings is not None