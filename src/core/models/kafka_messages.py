# core/models/kafka_messages.py
"""
Kafka message models for operational data.
Separate from worker result messages (WorkerResultMessage).

Topics:
- prod_component_metrics_topic: ComponentMetricsHealthMessage
- prod_worker_control_topic: WorkerControlMessage
- prod_worker_response_topic: ControlResponseMessage
"""

from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Literal, Optional, List, Union
from datetime import datetime, timezone
from uuid import uuid4
from enum import Enum


# ============================================================================
# ENUMS
# ============================================================================

class ComponentType(str, Enum):
    """Type of component in the system."""
    WORKER = "worker"
    ORCHESTRATOR = "orchestrator"
    WORKER_CONTROLLER = "worker_controller"


class HealthStatus(str, Enum):
    """Health status of a component."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class RegistrationEventType(str, Enum):
    """Event types for component lifecycle and metrics."""
    COMPONENT_REGISTERED = "COMPONENT_REGISTERED"
    COMPONENT_DEREGISTERED = "COMPONENT_DEREGISTERED"
    COMPONENT_HEARTBEAT = "COMPONENT_HEARTBEAT"
    METRICS_SNAPSHOT = "METRICS_SNAPSHOT"


class ControlCommandType(str, Enum):
    """Control commands that can be sent to components."""
    # Worker commands
    CONFIG_UPDATE_AVAILABLE = "CONFIG_UPDATE_AVAILABLE"
    SET_LOG_LEVEL = "SET_LOG_LEVEL"
    DRAIN = "DRAIN"
    RESUME = "RESUME"
    SHUTDOWN = "SHUTDOWN"
    HEARTBEAT_FORCE = "HEARTBEAT_FORCE"
    
    # WorkerController commands
    TEST_CONNECTION = "TEST_CONNECTION"
    TEST_DB_CONNECTION = "TEST_DB_CONNECTION"
    REQUEST_LOGS = "REQUEST_LOGS"
    RUN_DIAGNOSTIC = "RUN_DIAGNOSTIC"


# ============================================================================
# METRICS & HEALTH MESSAGES
# ============================================================================

class MetricsHealthMetadata(BaseModel):
    """Metadata for metrics and health messages."""
    message_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique message identifier"
    )
    event_type: RegistrationEventType = Field(
        ...,
        description="Type of event (METRICS_SNAPSHOT, HEARTBEAT, etc.)"
    )
    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp when message was created"
    )
    component_id: str = Field(
        ...,
        description="Unique identifier for this component instance"
    )
    component_type: ComponentType = Field(
        ...,
        description="Type of component (worker, orchestrator, worker_controller)"
    )
    nodegroup: str = Field(
        ...,
        description="NodeGroup this component belongs to (used as partition key)"
    )


class HealthCheckResult(BaseModel):
    """Result of a single health check."""
    check_name: str = Field(
        ...,
        description="Name of the health check (e.g., 'database_connection')"
    )
    status: HealthStatus = Field(
        ...,
        description="Status of this specific check"
    )
    message: str = Field(
        ...,
        description="Human-readable status message"
    )
    last_checked: str = Field(
        ...,
        description="ISO timestamp when check was last performed"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional check-specific data"
    )


class ComponentMetricsHealthMessage(BaseModel):
    """
    Combined metrics, health, registration, and heartbeat message.
    Published to prod_component_metrics_topic.
    
    Message structure varies by event_type:
    - COMPONENT_REGISTERED: version, capabilities
    - COMPONENT_DEREGISTERED: (minimal, just metadata)
    - COMPONENT_HEARTBEAT: health_status, health_checks
    - METRICS_SNAPSHOT: metrics, health_status, health_checks
    """
    metadata: MetricsHealthMetadata
    
    # Optional fields, present based on event_type
    metrics: Optional[Dict[str, Union[int, float, str]]] = Field(
        None,
        description="Key-value pairs of Prometheus-style metrics (for METRICS_SNAPSHOT)"
    )
    health_status: Optional[HealthStatus] = Field(
        None,
        description="Overall component health status (for HEARTBEAT, METRICS_SNAPSHOT)"
    )
    health_checks: Optional[List[HealthCheckResult]] = Field(
        None,
        description="Detailed results of individual health checks (for HEARTBEAT, METRICS_SNAPSHOT)"
    )
    version: Optional[str] = Field(
        None,
        description="Component software version (for COMPONENT_REGISTERED)"
    )
    capabilities: Optional[Dict[str, Any]] = Field(
        None,
        description="Component capabilities (for COMPONENT_REGISTERED)"
    )


# ============================================================================
# CONTROL COMMAND MESSAGES
# ============================================================================

class ControlMetadata(BaseModel):
    """Metadata for control command messages."""
    message_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique message identifier"
    )
    request_identifier: str = Field(
        ...,
        description="Unique ID for this command request, used to correlate response"
    )
    command_type: ControlCommandType = Field(
        ...,
        description="Type of command being issued"
    )
    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp when command was issued"
    )
    issued_by: str = Field(
        ...,
        description="User or system that issued the command"
    )


class WorkerControlMessage(BaseModel):
    """
    Command or notification message.
    Published to prod_worker_control_topic.
    Consumed by Workers and WorkerControllers.
    """
    metadata: ControlMetadata
    
    # Targeting
    target_nodegroup: str = Field(
        ...,
        description="NodeGroup to target (used as partition key)"
    )
    target_component_type: Optional[ComponentType] = Field(
        None,
        description="Filter by component type (worker, worker_controller) or null for all"
    )
    target_component_id: Optional[str] = Field(
        None,
        description="Specific component ID or null for all matching type/group"
    )
    
    # CONFIG_UPDATE_AVAILABLE pattern: indirect notification
    api_endpoint: Optional[str] = Field(
        None,
        description="Backend API endpoint to call for config details (for CONFIG_UPDATE_AVAILABLE)"
    )
    api_parameters: Optional[Dict[str, Any]] = Field(
        None,
        description="Parameters for the API call (for CONFIG_UPDATE_AVAILABLE)"
    )
    
    # Direct command parameters
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Command-specific parameters (e.g., log_level, duration, datasource_id)"
    )


# ============================================================================
# CONTROL RESPONSE MESSAGES
# ============================================================================

class ControlResponseMetadata(BaseModel):
    """Metadata for control response messages."""
    message_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique message identifier"
    )
    request_identifier: str = Field(
        ...,
        description="Correlates with the original WorkerControlMessage.metadata.request_identifier"
    )
    response_type: Literal["inline", "file", "status"] = Field(
        ...,
        description="Type of data in response_data (inline dict, S3 URL, or status only)"
    )
    status: Literal["Working", "Finished", "FinishedWithError"] = Field(
        ...,
        description="Current status of the command execution"
    )
    timestamp_utc: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO timestamp when response was created"
    )
    component_id: str = Field(
        ...,
        description="ID of the component sending this response"
    )
    nodegroup: str = Field(
        ...,
        description="NodeGroup of the responding component (used as partition key)"
    )


class ControlResponseMessage(BaseModel):
    """
    Response to a control command.
    Published to prod_worker_response_topic.
    """
    metadata: ControlResponseMetadata
    
    response_data: Optional[Dict[str, Any]] = Field(
        None,
        description="Result data (inline dict or {'s3_url': '...'})"
    )
    error_message: Optional[str] = Field(
        None,
        description="Error message if status is FinishedWithError"
    )
    
    @validator('error_message', always=True)
    def check_error_on_failure(cls, v, values):
        """Ensure error_message is present when status is FinishedWithError."""
        metadata = values.get('metadata')
        if metadata and metadata.status == "FinishedWithError" and not v:
            raise ValueError('error_message must be present when status is FinishedWithError')
        return v
        

class WorkerResultEventType(str, Enum):
    """
    Event types for worker result messages on prod_worker_results topic.
    Each event type corresponds to the Pydantic model defining the structure
    of the individual records within the message's payload array (or dedicated field).
    """
    CLASSIFICATION_FINDINGS = "CLASSIFICATION_FINDINGS" # Payload record type: ScanFindingSummary
    DISCOVERED_OBJECTS = "DISCOVERED_OBJECTS"           # Payload record type: DiscoveredObject
    TASK_OUTPUT_RECORDS = "TASK_OUTPUT_RECORDS"         # Signals data in TaskOutputRecords field (list of TaskOutputRecord)
    OBJECT_METADATA = "OBJECT_METADATA"                 # Payload record type: ObjectMetadata
    DATABASE_PROFILES = "DATABASE_PROFILES"             # Payload record type: SystemProfile
    BENCHMARK_FINDINGS = "BENCHMARK_FINDINGS"           # Payload record type: BenchmarkFinding
    ENTITLEMENT_SNAPSHOTS = "ENTITLEMENT_SNAPSHOTS"     # Payload record type: EntitlementSnapshot
    OBJECT_ACTION_RESULT = "OBJECT_ACTION_RESULT"       # Payload record type: ObjectActionResult
    TASK_STATUS_UPDATE = "TASK_STATUS_UPDATE"           # Signals data in TaskCompletionStatus field (type TaskStatusUpdatePayload)

    # RemediationLifeCycle specific event types
    REMEDIATION_GROUP_STATUS = "REMEDIATION_GROUP_STATUS"         # Payload record type: RemediationGroupStatusUpdate
    REMEDIATION_DATASOURCE_STATUS = "REMEDIATION_DATASOURCE_STATUS" # Payload record type: RemediationDataSourceStatusUpdate
    REMEDIATION_TABLE_STATUS = "REMEDIATION_TABLE_STATUS"           # Payload record type: RemediationTableStatusUpdate
    REMEDIATION_FINDING_STATUS = "REMEDIATION_FINDING_STATUS"       # Payload record type: RemediationFindingStatusUpdate

# --- Metadata Envelope Model ---

class WorkerResultMetadata(BaseModel):
    """
    Standard metadata envelope for all worker result messages.

    This metadata wraps all messages sent to prod_worker_results topic.
    The event_type field determines the structure of records in the main payload array.
    """
    message_id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="Unique UUID for this message instance"
    )
    event_type: WorkerResultEventType = Field(
        ...,
        description="Primary type of result data in payload array (or signals presence of TOR/Status)"
    )
    schema_version: str = Field(
        default="1.0", # Consider updating version based on changes
        description="Message schema version for backward compatibility"
    )
    job_id: int = Field(
        ...,
        description="Job ID that this result belongs to"
    )
    task_id: str = Field(
        ...,
        description="Task ID (hex string) that produced this result"
    )
    trace_id: Optional[str] = Field(
        None,
        description="Distributed tracing ID for correlation across services"
    )
    worker_id: str = Field(
        ...,
        description="Worker component ID that produced this result"
    )
    datasource_id: str = Field(
        ...,
        description="Datasource ID (used as Kafka partition key)"
    )
    timestamp_utc: str = Field(
        # Use timezone.utc for consistency
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp (UTC) when message was created"
    )
    # Renamed record_count for clarity with optional payload
    payload_record_count: Optional[int] = Field(
        None, # Optional now, depends if payload is present
        description="Number of records in the main 'payload' array, if present."
    )
    message_sequence: int = Field(
        ...,
        ge=1,
        description="Message sequence number within task (1, 2, 3, ...)"
    )
    is_final_message: bool = Field(
        ...,
        description="True if this is the last message containing *any* data or status for the task"
    )
    total_messages_in_task: Optional[int] = Field(
        None,
        description="Total message count for task (only present when is_final_message=true)"
    )

# --- Full Message Envelope Model ---

class WorkerResultMessage(BaseModel):
    """
    Complete message envelope for prod_worker_results topic.

    Can contain primary payload based on event_type, optional TaskOutputRecords,
    and an optional TaskCompletionStatus (which MUST be present if
    metadata.is_final_message is true).
    """
    metadata: WorkerResultMetadata

    # Primary payload (optional, depends on event_type and if data exists in this message)
    payload: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Array of primary result records (type varies by event_type, e.g., ScanFindingSummary, DiscoveredObject). May be null."
    )

    # Optional Task Output Records (payload structure should match the TaskOutputRecord Pydantic model)
    TaskOutputRecords: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Optional array containing TaskOutputRecord data structures."
    )

    # Optional Task Completion Status (Mandatory if final message)
    # Using the TaskStatusUpdatePayload model directly
    TaskCompletionStatus: Optional[TaskStatusUpdatePayload] = Field(
        None,
        description="Contains the final status, summary, and errors. MUST be present if metadata.is_final_message is true."
    )

    # Validator to enforce TaskCompletionStatus on final message
    @validator('TaskCompletionStatus', always=True)
    def check_status_on_final_message(cls, v, values):
        metadata = values.get('metadata')
        # Check if metadata exists and is_final_message is True
        if metadata and getattr(metadata, 'is_final_message', False) and v is None:
            raise ValueError('TaskCompletionStatus MUST be present when metadata.is_final_message is true')
        return v

    # Validator to ensure at least one payload field is present if not final message? (Optional, depends on strictness)
    # @validator('payload', always=True)
    # def check_payload_presence(cls, v, values):
    #     metadata = values.get('metadata')
    #     task_outputs = values.get('TaskOutputRecords')
    #     task_status = values.get('TaskCompletionStatus')
    #     is_final = metadata and getattr(metadata, 'is_final_message', False)
    #     # If not the final message, at least one data field should ideally be present
    #     if not is_final and v is None and task_outputs is None:
    #          # Might allow empty progress messages, log a warning instead?
    #          # raise ValueError('At least one payload field (payload or TaskOutputRecords) must be present if not the final message')
    #          pass # Allow empty messages for now
    #     # Ensure payload_record_count matches payload length if payload exists
    #     if v is not None and metadata and metadata.payload_record_count != len(v):
    #          raise ValueError(f"payload_record_count ({metadata.payload_record_count}) does not match actual payload length ({len(v)})")
    #     elif v is None and metadata and metadata.payload_record_count is not None and metadata.payload_record_count != 0:
    #          raise ValueError("payload_record_count is non-zero but payload is null")
    #     return v


    class Config:
        # Configuration for Pydantic, e.g., allowing bytes encoding
        json_encoders = {
            bytes: lambda b: base64.b64encode(b).decode('ascii') if b else None
        }
        # Use enum values in serialization if desired
        use_enum_values = True
        # Validate on assignment
        validate_assignment = True

