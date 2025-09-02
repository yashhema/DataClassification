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

from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Annotated
from uuid import uuid4
from dataclasses import dataclass, field
from pydantic import BaseModel, Field

# =============================================================================
# Enums
# =============================================================================

class TaskType(str, Enum):
    """Defines the specific type of work to be performed."""
    DISCOVERY_ENUMERATE = "DISCOVERY_ENUMERATE"
    DISCOVERY_GET_DETAILS = "DISCOVERY_GET_DETAILS"
    CLASSIFICATION = "CLASSIFICATION"
    DELTA_CALCULATE = "DELTA_CALCULATE"

class ObjectType(str, Enum):
    """Defines the type of the object being processed."""
    FILE = "FILE"
    DIRECTORY = "DIRECTORY"
    DATABASE_TABLE = "DATABASE_TABLE"

# =============================================================================
# Core Data Contracts (Used across multiple components)
# =============================================================================

class DiscoveredObject(BaseModel):
    """A lightweight record of an object found during enumeration."""
    object_id: str = Field(..., description="Unique hash-based identifier for the object.")
    object_type: ObjectType
    object_path: str = Field(..., description="Full path or identifier of the object at the source.")
    size_bytes: int = 0
    created_date: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    discovery_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ObjectMetadata(BaseModel):
    """A comprehensive record holding detailed metadata for an object."""
    base_object: DiscoveredObject
    detailed_metadata: Dict[str, Any] = Field(..., description="A flexible dict for type-specific details like permissions or table schemas.")
    metadata_fetch_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PIIFinding(BaseModel):
    """Represents a single instance of a sensitive data finding."""
    finding_id: str = Field(default_factory=lambda: f"finding_{uuid4().hex}")
    entity_type: str
    text: str
    start_position: int
    end_position: int
    confidence_score: float
    classifier_id: str

# =============================================================================
# WorkPacket Structure (The contract between Orchestrator and Worker)
# =============================================================================

class WorkPacketHeader(BaseModel):
    """Standard header included in every WorkPacket for context and tracing."""
    task_id: int
    job_id: int
    trace_id: str = Field(default_factory=lambda: f"trace_{uuid4().hex}")
    created_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class TaskConfig(BaseModel):
    """A flexible container for task-specific operational parameters."""
    batch_write_size: int = 1000
    max_content_size_mb: int = 100
    fetch_permissions: bool = True
    # Add other relevant system parameters here

# --- Payloads for each TaskType (FIXED: Added discriminator fields) ---

class DiscoveryEnumeratePayload(BaseModel):
    """Payload for DISCOVERY_ENUMERATE tasks."""
    task_type: TaskType = Field(default=TaskType.DISCOVERY_ENUMERATE, description="Discriminator field for Union validation")
    datasource_id: str
    paths: List[str]
    staging_table_name: str

class DiscoveryGetDetailsPayload(BaseModel):
    """Payload for DISCOVERY_GET_DETAILS tasks."""
    task_type: TaskType = Field(default=TaskType.DISCOVERY_GET_DETAILS, description="Discriminator field for Union validation")
    datasource_id: str
    object_ids: List[str]

class ClassificationPayload(BaseModel):
    """Payload for CLASSIFICATION tasks."""
    task_type: TaskType = Field(default=TaskType.CLASSIFICATION, description="Discriminator field for Union validation")
    datasource_id: str
    classifier_template_id: str
    object_ids: List[str]

class DeltaCalculatePayload(BaseModel):
    """Payload for DELTA_CALCULATE tasks."""
    task_type: TaskType = Field(default=TaskType.DELTA_CALCULATE, description="Discriminator field for Union validation")
    staging_table_name: str

# FIXED: Discriminated Union for WorkPacket payload
class WorkPacket(BaseModel):
    """The final, consolidated WorkPacket model with proper Union validation."""
    header: WorkPacketHeader
    config: TaskConfig
    payload: Union[
        Annotated[DiscoveryEnumeratePayload, Field(discriminator='task_type')],
        Annotated[DiscoveryGetDetailsPayload, Field(discriminator='task_type')],
        Annotated[ClassificationPayload, Field(discriminator='task_type')],
        Annotated[DeltaCalculatePayload, Field(discriminator='task_type')]
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
    task_id: int
    progress_percentage: Optional[float] = None
    items_processed: Optional[int] = None
    items_total: Optional[int] = None
    status_message: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# =============================================================================
# Validation and Helper Functions
# =============================================================================

def create_work_packet(
    task_id: int,
    job_id: int,
    payload_type: TaskType,
    payload_data: Dict[str, Any],
    trace_id: Optional[str] = None,
    config_overrides: Optional[Dict[str, Any]] = None
) -> WorkPacket:
    """
    Helper function to create properly validated WorkPacket instances.
    
    Args:
        task_id: The task identifier
        job_id: The job identifier  
        payload_type: The type of task payload to create
        payload_data: The data for the payload
        trace_id: Optional trace ID for request tracing
        config_overrides: Optional task configuration overrides
    
    Returns:
        A validated WorkPacket instance
    
    Raises:
        ValueError: If payload_type doesn't match payload_data structure
    """
    # Create header
    header = WorkPacketHeader(
        task_id=task_id,
        job_id=job_id,
        trace_id=trace_id or f"trace_{uuid4().hex}"
    )
    
    # Create config with any overrides
    config_data = {"batch_write_size": 1000, "max_content_size_mb": 100, "fetch_permissions": True}
    if config_overrides:
        config_data.update(config_overrides)
    config = TaskConfig(**config_data)
    
    # Create appropriate payload based on type
    payload_data_with_type = {**payload_data, "task_type": payload_type}
    
    if payload_type == TaskType.DISCOVERY_ENUMERATE:
        payload = DiscoveryEnumeratePayload(**payload_data_with_type)
    elif payload_type == TaskType.DISCOVERY_GET_DETAILS:
        payload = DiscoveryGetDetailsPayload(**payload_data_with_type)
    elif payload_type == TaskType.CLASSIFICATION:
        payload = ClassificationPayload(**payload_data_with_type)
    elif payload_type == TaskType.DELTA_CALCULATE:
        payload = DeltaCalculatePayload(**payload_data_with_type)
    else:
        raise ValueError(f"Unsupported payload type: {payload_type}")
    
    # Create and validate the complete WorkPacket
    return WorkPacket(header=header, config=config, payload=payload)

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