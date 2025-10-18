# src/core/models/models.py
"""
Defines the core Pydantic data models for the entire system.
These models serve as the definitive, type-safe data contracts for
WorkPackets, results, and component interfaces.

FIXES APPLIED:
- Added discriminator fields to payload classes for proper Union validation
- Enhanced WorkPacket with proper Pydantic discriminated union
- Maintained all existing functionality and data structures
"""
import base64
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Annotated,Literal
from uuid import uuid4
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
from .querymodel import SelectionCriteria,QueryDefinition,Pagination


# =============================================================================
# Enums
# =============================================================================

    
class TaskType(str, Enum):
    """Defines the specific type of work to be performed."""
    DISCOVERY_ENUMERATE = "DISCOVERY_ENUMERATE"
    DISCOVERY_GET_DETAILS = "DISCOVERY_GET_DETAILS"
    CLASSIFICATION = "CLASSIFICATION"
    DELTA_CALCULATE = "DELTA_CALCULATE"
    # NEW: Remediation Task Types

    # --- Policy Job Workflow Tasks ---
    POLICY_SELECTOR_PLAN = "POLICY_SELECTOR_PLAN"
    POLICY_SELECTOR_EXECUTE = "POLICY_SELECTOR_EXECUTE"
    POLICY_COMMIT_PLAN = "POLICY_COMMIT_PLAN"
    POLICY_ENRICHMENT = "POLICY_ENRICHMENT"
    POLICY_ACTION_EXECUTE = "POLICY_ACTION_EXECUTE"
    POLICY_RECONCILE = "POLICY_RECONCILE"    
    # New types...
    DATASOURCE_PROFILE = "DATASOURCE_PROFILE"
    BENCHMARK_EXECUTE = "BENCHMARK_EXECUTE"
    ENTITLEMENT_EXTRACT = "ENTITLEMENT_EXTRACT"
    VULNERABILITY_SCAN = "VULNERABILITY_SCAN"    
    

class ObjectType(str, Enum):
    """Defines the type of the object being processed."""
    FILE = "FILE"
    DIRECTORY = "DIRECTORY"
    DATABASE_TABLE = "DATABASE_TABLE"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"

# =============================================================================
# Remediation Results returned from connector
# =============================================================================
class FailedObject(BaseModel):
    path: str
    error_message: str

class RemediationResult(BaseModel):
    succeeded_paths: List[str]
    failed_paths: List[FailedObject]
    success_count: int
    failure_count: int


# =============================================================================
# DataSource Profile returned from connector
# =============================================================================
class SQLServerProfileExtension(BaseModel):
    """Vendor-specific details for SQL Server."""
    model_type: Literal["sqlserver"] = "sqlserver"
    edition: str
    engine_edition: int
    host_platform: str
    product_level: str  # e.g., RTM, SP1, CU18
    collation: Optional[str]
    is_clustered: bool
    is_hadr_enabled: bool
    compatibility_level: Optional[int]

class PostgreSQLProfileExtension(BaseModel):
    """Vendor-specific details for PostgreSQL."""
    model_type: Literal["postgres"] = "postgres"
    server_encoding: str
    server_version_num: int # e.g., 150003 for 15.3
    is_in_recovery: bool # True if the server is a read-replica
    data_directory: Optional[str]

# Phase 2: SQL Server and PostgreSQL only
# TODO: Add MySQL, Oracle, MongoDB extensions in future phases
VendorProfileExtension = Annotated[
    Union[SQLServerProfileExtension, PostgreSQLProfileExtension],
    Field(discriminator="model_type")
]

class SystemProfile(BaseModel):
    """
    A standardized model for a data source's discovered profile,
    including version, OS, and vendor-specific capabilities.
    """
    product_name: str = Field(..., description="e.g., 'sqlserver', 'postgres'")
    full_version: str = Field(..., description="The full, raw version string from the source, e.g., '15.0.4280.7'.")
    normalized_version: str = Field(..., description="A zero-padded, standardized version string for reliable comparison, e.g., '015.000.04280.007'.")
    version_parts: Dict[str, int] = Field(..., description="Structured version components: major, minor, build, revision")
    patch_identifier: Optional[str] = Field(None, description="The human-readable patch level, e.g., 'CU18' or '15.3'.")
    release_date: Optional[str] = Field(None, description="Release date of this version/patch if known, ISO format")
    deployment_model: str = Field(..., description="e.g., 'SELF_MANAGED', 'CLOUD_MANAGED', 'SAAS'")
    vendor_specific_details: Optional[VendorProfileExtension] = None

class DictionaryRule(BaseModel):
    """A rule consisting of a list of values and an associated confidence boost. used by dictionary table fields"""
    vallst: List[str] = Field(..., description="The list of keywords or values to match.")
    boost: float = Field(..., description="The confidence boost to apply if a match is found.")
    


# --- Sub-models for Rule Parameters ---

class BoostTier(BaseModel):
    """Defines a confidence boost for a specific number of categories found."""
    categories_found: int
    confidence_boost: float

class ValueMatchKeyword(BaseModel):
    """Defines a specific boost for an individual keyword."""
    value: str
    boost: float

# --- Main Rule Models ---

class ColNameMatchRule(BaseModel):
    """Rule to boost confidence if the column's name matches a keyword."""
    rule_type: Literal["COL_NAME_MATCH"]
    rule_scope: Literal["all"]
    description: Optional[str] = None
    parameters: dict[Literal["keywords", "confidence_boost"], Union[List[str], float]]

class CategoryCoOccurrenceRule(BaseModel):
    """Rule to boost confidence if other PII categories are in the same row."""
    rule_type: Literal["CATEGORY_CO_OCCURRENCE"]
    rule_scope: Literal["row"]
    description: Optional[str] = None
    parameters: dict[Literal["target_categories", "boost_tiers"], Union[List[str], List[BoostTier]]]

class ValueMatchRule(BaseModel):
    """Rule to boost confidence if specific keywords are in other columns of the same row."""
    rule_type: Literal["VALUE_MATCH_IN_COLUMN_VALUE"]
    rule_scope: Literal["row"]
    description: Optional[str] = None
    parameters: dict[Literal["keywords"], List[ValueMatchKeyword]]

# --- The Discriminated Union and Final Container ---

# This tells Pydantic to use the "rule_type" field to determine which model to use
CrossColumnRule = Annotated[
    Union[ColNameMatchRule, CategoryCoOccurrenceRule, ValueMatchRule],
    Field(discriminator="rule_type")
]

class CrossColumnSupport(BaseModel):
    """The top-level model for the cross_column_support field."""
    enabled: bool
    rules: List[CrossColumnRule]

# =============================================================================
# Core Data Contracts (Used across multiple components)
# =============================================================================

class DiscoveredObject(BaseModel):
    """A lightweight record of an object found during enumeration."""
    object_key_hash: bytes = Field(..., description="Unique SHA-256 hash for the object.")
    datasource_id: str # FIX: Added the missing datasource_id field
    object_type: ObjectType
    object_path: str = Field(..., description="Full path or identifier of the object at the source.")
    size_bytes: int = 0
    created_date: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    last_accessed: Optional[datetime] = None
    discovery_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        json_encoders = {
            bytes: lambda v: base64.b64encode(v).decode('ascii') if v else None
        }

class BoundaryType(str, Enum):
    """Defines the logical boundary for a single enumeration task."""
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    DIRECTORY = "DIRECTORY"

class DiscoveryBatch(BaseModel):
    """The data contract for a streamed batch of discovered objects from a connector."""
    boundary_id: bytes = Field(..., description="A unique hash of the boundary (e.g., directory path or database name) being processed.")
    is_final_batch: bool = Field(..., description="True only if this is the last batch for the specified boundary.")
    discovered_objects: List[DiscoveredObject]
    boundary_path:str

    class Config:
        json_encoders = {
            bytes: lambda v: base64.b64encode(v).decode('ascii') if v else None
        }

class ObjectMetadata(BaseModel):
    """A comprehensive record holding detailed metadata for an object."""
    base_object: DiscoveredObject
    detailed_metadata: Dict[str, Any] = Field(..., description="A flexible dict for type-specific details like permissions or table schemas.")
    metadata_fetch_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    @property
    def object_key_hash(self) -> bytes:
        """Computed property for database operations"""
        return self.base_object.object_key_hash    

class PIIFinding(BaseModel):
    """Represents a single instance of a sensitive data finding."""
    finding_id: str = Field(default_factory=lambda: f"finding_{uuid4().hex}")
    entity_type: str
    text: str
    start_position: int
    end_position: int
    confidence_score: float
    classifier_id: str
    context_data: Dict[str, Any] = Field(default_factory=dict)
    

# =============================================================================
# WorkPacket Structure (The contract between Orchestrator and Worker)
# =============================================================================

class WorkPacketHeader(BaseModel):
    """Standard header included in every WorkPacket for context and tracing."""
    task_id: str
    job_id: int
    parent_task_id: Optional[str] = None
    trace_id: str = Field(default_factory=lambda: f"trace_{uuid4().hex}")
    created_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class TaskConfig(BaseModel):
    """A flexible container for task-specific operational parameters."""
    batch_write_size: int = 1000
    max_content_size_mb: int = 100
    fetch_permissions: bool = True
    retry_count: int = Field(0, description="The number of times this task has been attempted.")
    # Add other relevant system parameters here

# --- Specific, Type-Safe Action Definitions ---
class ActionType(str, Enum):
    MOVE = "MOVE"
    DELETE = "DELETE"
    TAG = "TAG"
    ENCRYPT = "ENCRYPT"
    MIP = "MIP"

class BaseAction(BaseModel):
    action_type: ActionType

class TombstoneConfig(BaseModel):
    message: str
    filename_format: str

class MoveAction(BaseAction):
    action_type: Literal[ActionType.MOVE]
    destination_directory: str
    requires_tombstone: bool = False
    tombstone_config: Optional[TombstoneConfig] = None

class DeleteAction(BaseAction):
    action_type: Literal[ActionType.DELETE]
    requires_tombstone: bool = False
    tombstone_config: Optional[TombstoneConfig] = None
    
class TagAction(BaseAction):
    action_type: Literal[ActionType.TAG]
    tags_to_add: List[str]

class EncryptAction(BaseAction):
    action_type: Literal[ActionType.ENCRYPT]
    encryption_key_id: str

class MIPAction(BaseAction):
    action_type: Literal[ActionType.MIP]
    classifier_to_mip_label_map: Dict[str, str]

ActionDefinition = Annotated[
    Union[MoveAction, DeleteAction, TagAction, EncryptAction, MIPAction],
    Field(discriminator="action_type")
]

# --- Payloads for each TaskType (FIXED: Added discriminator fields) ---

class DiscoveryEnumeratePayload(BaseModel):
    """Payload for DISCOVERY_ENUMERATE tasks."""
    task_type: Literal[TaskType.DISCOVERY_ENUMERATE] = Field(default=TaskType.DISCOVERY_ENUMERATE, description="Discriminator field for Union validation")
    datasource_id: str
    paths: List[str]
    staging_table_name: str
    

class DiscoveryGetDetailsPayload(BaseModel):
    """Payload for DISCOVERY_GET_DETAILS tasks."""
    task_type: Literal[TaskType.DISCOVERY_GET_DETAILS] = Field(default=TaskType.DISCOVERY_GET_DETAILS, description="Discriminator field for Union validation")
    datasource_id: str
    discovered_objects: List[DiscoveredObject]

class ClassificationPayload(BaseModel):
    """Payload for CLASSIFICATION tasks."""
    task_type: Literal[TaskType.CLASSIFICATION] = Field(default=TaskType.CLASSIFICATION, description="Discriminator field for Union validation")
    datasource_id: str
    classifier_template_id: str
    discovered_objects: List[DiscoveredObject]

class DeltaCalculatePayload(BaseModel):
    """Payload for DELTA_CALCULATE tasks."""
    task_type: Literal[TaskType.DELTA_CALCULATE] = Field(default=TaskType.DELTA_CALCULATE, description="Discriminator field for Union validation")
    staging_table_name: str

class PolicyConfiguration(BaseModel):
    selection_criteria: SelectionCriteria
    action_definition: ActionDefinition


class PolicySelectorPlanPayload(BaseModel):
    task_type: Literal[TaskType.POLICY_SELECTOR_PLAN]
    plan_id: str # Unique ID for the entire remediation plan
    policy_config: PolicyConfiguration # The complete, strongly-typed policy

class PolicySelectorExecutePayload(BaseModel):
    task_type: Literal[TaskType.POLICY_SELECTOR_EXECUTE]
    plan_id: str
    query: QueryDefinition
    pagination: Pagination # Each worker gets its own specific page to query

class ObjectToProcess(BaseModel):
    ObjectID: str
    ObjectPath: str

class PolicyActionExecutePayload(BaseModel):
    task_type: Literal[TaskType.POLICY_ACTION_EXECUTE]
    plan_id: str
    bin_id: str # The specific "bin" this worker is responsible for
    action: ActionDefinition # The strongly-typed action to perform
    # The payload contains the explicit list of objects for this worker
    objects_to_process: List[ObjectToProcess]

class PolicyCommitPlanPayload(BaseModel):
    """
    Payload for the task that validates and commits a selection plan,
    acting as a gatekeeper before the action phase.
    """
    task_type: Literal[TaskType.POLICY_COMMIT_PLAN] = Field(default=TaskType.POLICY_COMMIT_PLAN)
    plan_id: str
    action_definition: ActionDefinition

class PolicyEnrichmentPayload(BaseModel):
    """Payload for an optional task to enrich objects in a plan."""
    task_type: Literal[TaskType.POLICY_ENRICHMENT] = Field(default=TaskType.POLICY_ENRICHMENT)
    plan_id: str
    tags_to_add: List[str]

class PolicyReconcilePayload(BaseModel):
    """
    Payload for the final task that updates the master catalog with the
    results from the RemediationLedger.
    """
    task_type: Literal[TaskType.POLICY_RECONCILE] = Field(default=TaskType.POLICY_RECONCILE)
    plan_id: str
    updates: List[Dict[str, Any]] = Field(..., description="The list of updates to apply to the metadata catalog.")

class DatasourceProfilePayload(BaseModel):
    task_type: Literal[TaskType.DATASOURCE_PROFILE] = Field(default=TaskType.DATASOURCE_PROFILE)
    datasource_id: str

class BenchmarkExecutePayload(BaseModel):
    """Payload for BENCHMARK_EXECUTE tasks."""
    task_type: Literal[TaskType.BENCHMARK_EXECUTE] = TaskType.BENCHMARK_EXECUTE
    datasource_id: str
    cycle_id: str
    benchmark_name: str


class VulnerabilityScanPayload(BaseModel):
    """Payload for VULNERABILITY_SCAN tasks."""
    task_type: Literal[TaskType.VULNERABILITY_SCAN] = TaskType.VULNERABILITY_SCAN
    datasource_id: str
    cycle_id: str
    # The full profile is copied into the task
    system_profile_snapshot: Dict[str, Any] = Field(..., description="Snapshot of SystemProfile at job creation time for audit trail")

class EntitlementExtractPayload(BaseModel):
    """Payload for ENTITLEMENT_EXTRACT tasks."""
    task_type: Literal[TaskType.ENTITLEMENT_EXTRACT] = TaskType.ENTITLEMENT_EXTRACT
    datasource_id: str
    cycle_id: str

#


# FIXED: Discriminated Union for WorkPacket payload
class WorkPacket(BaseModel):
    """The final, consolidated WorkPacket model with proper Union validation."""
    header: WorkPacketHeader
    config: TaskConfig
    payload: Union[
        Annotated[DiscoveryEnumeratePayload, Field(discriminator='task_type')],
        Annotated[DiscoveryGetDetailsPayload, Field(discriminator='task_type')],
        Annotated[ClassificationPayload, Field(discriminator='task_type')],
        Annotated[DeltaCalculatePayload, Field(discriminator='task_type')],

        # NEW: Added Policy Job payloads to the discriminated union
        Annotated[PolicySelectorPlanPayload, Field(discriminator='task_type')],
        Annotated[PolicySelectorExecutePayload, Field(discriminator='task_type')],
        Annotated[PolicyEnrichmentPayload, Field(discriminator='task_type')],
        Annotated[PolicyActionExecutePayload, Field(discriminator='task_type')],
        Annotated[PolicyCommitPlanPayload, Field(discriminator='task_type')],
        Annotated[PolicyEnrichmentPayload, Field(discriminator='task_type')],
        
        Annotated[PolicyReconcilePayload, Field(discriminator='task_type')],
        Annotated[DatasourceProfilePayload, Field(discriminator='task_type')],
        Annotated[BenchmarkExecutePayload, Field(discriminator='task_type')],
        Annotated[EntitlementExtractPayload, Field(discriminator='task_type')],
        Annotated[VulnerabilityScanPayload, Field(discriminator='task_type')]

    ]

    class Config:
        """Pydantic configuration for better Union handling."""
        use_enum_values = True  # Use enum values in serialization
        validate_assignment = True  # Validate on assignment

    def get_task_type(self) -> TaskType:
        """Convenience method to get the task type from the payload."""
        return self.payload.task_type

    def is_discovery_task(self) -> bool:
        """Check if this is a discovery-related task."""
        return self.payload.task_type in [TaskType.DISCOVERY_ENUMERATE, TaskType.DISCOVERY_GET_DETAILS]

    def is_classification_task(self) -> bool:
        """Check if this is a classification task."""
        return self.payload.task_type == TaskType.CLASSIFICATION

    def get_datasource_id(self) -> Optional[str]:
        """Get the datasource ID if the payload has one."""
        if hasattr(self.payload, 'datasource_id'):
            return self.payload.datasource_id
        return None

# =============================================================================
# Result & Output Contracts (Used in interfaces)
# =============================================================================

class EnumerationResult(BaseModel):
    """The final output from a successful enumeration task."""
    total_objects_found: int
    new_sub_task_definitions: List[Dict[str, Any]] = Field(default_factory=list)

class GetDetailsResult(BaseModel):
    """The final output from a successful get details task."""
    success_count: int
    failed_count: int

class ClassificationResult(BaseModel):
    """The final output from a successful classification task."""
    objects_processed: int
    total_findings_found: int

# =============================================================================
# Task Output and Progress Models
# =============================================================================

class TaskOutputRecord(BaseModel):
    """Model for task output records used in pipelining."""
    output_type: str
    
    output_payload: Dict[str, Any]
    batch_id: Optional[str] = None
    count: Optional[int] = None
    
    class Config:
        """Allow extra fields for flexibility."""
        extra = "allow"

class ProgressUpdate(BaseModel):
    """Model for progress updates sent by workers."""
    task_id: str
    progress_percentage: Optional[float] = None
    items_processed: Optional[int] = None
    items_total: Optional[int] = None
    status_message: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# =============================================================================
# Validation and Helper Functions
# =============================================================================

#Only used for testing
def create_work_packet(
    task_id: str,
    job_id: int,
    parent_task_id:str,
    task_type: TaskType,
    payload_data: Dict[str, Any],
    trace_id: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None
) -> WorkPacket:
    """
    Helper function to create properly validated WorkPacket instances. This function
    is essential for testing and can be used by the Orchestrator to build tasks.
    """
    header = WorkPacketHeader(
        task_id=task_id,
        job_id=job_id,
        trace_id=trace_id or f"trace_{uuid4().hex}"
    )
    
    config_data = {"batch_write_size": 1000, "max_content_size_mb": 100, "fetch_permissions": True}
    if config_overrides:
        config_data.update(config_overrides)
    config = TaskConfig(**config_data)
    
    # Add the discriminator field to the payload data for validation
    payload_data_with_type = {**payload_data, "task_type": task_type}
    
    # UPDATED: The map now includes all new Policy and Scanning task types.
    payload_class_map = {
        # Scanning Payloads
        TaskType.DISCOVERY_ENUMERATE: DiscoveryEnumeratePayload,
        TaskType.DISCOVERY_GET_DETAILS: DiscoveryGetDetailsPayload,
        TaskType.CLASSIFICATION: ClassificationPayload,
        TaskType.DELTA_CALCULATE: DeltaCalculatePayload,
        
        # Policy Payloads
        TaskType.POLICY_SELECTOR_PLAN: PolicySelectorPlanPayload,
        TaskType.POLICY_SELECTOR_EXECUTE: PolicySelectorExecutePayload,
        TaskType.POLICY_COMMIT_PLAN: PolicyCommitPlanPayload,
        TaskType.POLICY_ENRICHMENT: PolicyEnrichmentPayload,
        TaskType.POLICY_ACTION_EXECUTE: PolicyActionExecutePayload,
        TaskType.POLICY_RECONCILE: PolicyReconcilePayload,
        TaskType.DATASOURCE_PROFILE: DatasourceProfilePayload,
        TaskType.BENCHMARK_EXECUTE: BenchmarkExecutePayload,
        TaskType.ENTITLEMENT_EXTRACT: EntitlementExtractPayload,
        TaskType.VULNERABILITY_SCAN: VulnerabilityScanPayload        
    }
    
    payload_class = payload_class_map.get(task_type)
    if not payload_class:
        raise ValueError(f"Unsupported payload type in create_work_packet: {task_type}")
    
    payload = payload_class(**payload_data_with_type)
    
    return WorkPacket(header=header, config=config, payload=payload)
def create_sample_work_packets() -> List[WorkPacket]:
    """Create sample WorkPacket instances for all major types for testing."""
    samples = []
    
    # Standard Discovery task
    samples.append(create_work_packet(1, 101, TaskType.DISCOVERY_ENUMERATE, {
        "datasource_id": "ds_001",
        "paths": ["/data/folder1"],
        "staging_table_name": "staging_job_101"
    }))
    
    # Policy Selector Plan task
    samples.append(create_work_packet(2, 102, TaskType.POLICY_SELECTOR_PLAN, {
        "query_definition": {"filters": [{"field": "EntityType", "value": "US_SSN"}]}
    }))

    # Policy Action Execute task
    samples.append(create_work_packet(3, 102, TaskType.POLICY_ACTION_EXECUTE, {
        "ledger_plan_id": 55,
        "action": {
            "action_type": ActionType.MOVE_AND_TOMBSTONE,
            "destination_directory": "/secure_archive/ssn_files",
            "tombstone_summary": "File moved due to PII policy violation (US_SSN)."
        }
    }))
    
    return samples



def validate_work_packet_json(json_data: str) -> WorkPacket:
    """
    Validate a JSON string as a WorkPacket.
    
    Args:
        json_data: JSON string representation of a WorkPacket
        
    Returns:
        Validated WorkPacket instance
        
    Raises:
        ValidationError: If JSON doesn't match WorkPacket schema
    """
    import json
    from pydantic import ValidationError
    
    try:
        data = json.loads(json_data)
        return WorkPacket(**data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")
    except ValidationError as e:
        raise ValueError(f"WorkPacket validation failed: {e}")


# =============================================================================
# Datasource extraction related config and model
# =============================================================================
@dataclass
class ContentComponent:
    """Universal component model for all extracted content types"""
    object_id: str
    component_type: str  
    component_id: str
    parent_path: str
    content: Union[str, bytes]
    original_size: int
    extracted_size: int
    is_truncated: bool
    schema: Dict[str, Any]
    metadata: Dict[str, Any]
    extraction_method: str
    is_archive_extraction: bool = False

@dataclass
class ContentExtractionLimits:
    max_file_size_mb: int = 10
    max_component_size_mb: int = 5
    max_text_chars: int = 100000
    max_document_table_rows: int = 1000
    max_archive_members: int = 50
    max_archive_depth: int = 3
    sampling_strategy: str = "head"

@dataclass 
class ContentExtractionFeatures:
    extract_tables: bool = True
    extract_pictures: bool = False
    extract_archives: bool = True
    ocr_enabled: bool = False
    preserve_structure: bool = True
    include_metadata: bool = True
    treat_xml_json_structured: bool = True
    tika_fallback_enabled: bool = False
    cleanup_temp_files: bool = False # Default to False for safety
    mount_required: bool = False
@dataclass
class ContentExtractionConfig:
    limits: ContentExtractionLimits = field(default_factory=ContentExtractionLimits)
    features: ContentExtractionFeatures = field(default_factory=ContentExtractionFeatures)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ContentExtractionConfig':
        limits_dict = config_dict.get('limits', {})
        features_dict = config_dict.get('features', {})
        
        limits = ContentExtractionLimits(**limits_dict)
        features = ContentExtractionFeatures(**features_dict)
        
        return cls(limits=limits, features=features)
# =============================================================================
# Example Usage and Testing
# =============================================================================

def create_sample_work_packets() -> List[WorkPacket]:
    """Create sample WorkPacket instances for testing."""
    
    samples = []
    
    # Discovery enumeration task
    discovery_payload = {
        "datasource_id": "ds_001",
        "paths": ["/data/folder1", "/data/folder2"],
        "staging_table_name": "staging_discovered_objects_job_123"
    }
    samples.append(create_work_packet(1, 100, TaskType.DISCOVERY_ENUMERATE, discovery_payload))
    
    # Get details task
    details_payload = {
        "datasource_id": "ds_001", 
        "object_ids": ["obj_001", "obj_002", "obj_003"]
    }
    samples.append(create_work_packet(2, 100, TaskType.DISCOVERY_GET_DETAILS, details_payload))
    
    # Classification task
    classification_payload = {
        "datasource_id": "ds_001",
        "classifier_template_id": "pii_financial",
        "object_ids": ["obj_001", "obj_002"]
    }
    samples.append(create_work_packet(3, 100, TaskType.CLASSIFICATION, classification_payload))
    
    # Delta calculation task
    delta_payload = {
        "staging_table_name": "staging_discovered_objects_job_123"
    }
    samples.append(create_work_packet(4, 100, TaskType.DELTA_CALCULATE, delta_payload))
    
    return samples

if __name__ == "__main__":
    # Test the models
    samples = create_sample_work_packets()
    
    for i, packet in enumerate(samples):
        print(f"Sample {i+1}: {packet.get_task_type()} task")
        print(f"  Datasource: {packet.get_datasource_id()}")
        print(f"  Is Discovery: {packet.is_discovery_task()}")
        print(f"  Is Classification: {packet.is_classification_task()}")
        print()