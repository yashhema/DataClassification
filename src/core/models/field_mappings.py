"""
Canonical field name mappings for query builder.
Maps user-facing filter field names to actual DB/Parquet column names.
"""

from typing import Dict, Literal, Any

DataSourceType = Literal["DISCOVEREDOBJECT", "OBJECTMETADATA", "SCANFINDINGS", "POLICY_ACTION_RESULTS"]

class FieldMetadata:
    """Metadata about a queryable field"""
    def __init__(
        self, 
        column_name: str, 
        data_type: str,
        supports_text_search: bool = False,
        athena_column: str = None,
        yugabyte_column: str = None
    ):
        self.column_name = column_name
        self.data_type = data_type
        self.supports_text_search = supports_text_search
        # Allow different column names per backend if needed
        self.athena_column = athena_column or column_name
        self.yugabyte_column = yugabyte_column or column_name

# Field mappings per data source
FIELD_MAPPINGS: Dict[DataSourceType, Dict[str, FieldMetadata]] = {
    "DISCOVEREDOBJECT": {
        "object_id": FieldMetadata("object_key_hash", "bytes"),
        "file_path": FieldMetadata("object_path", "str", supports_text_search=True),
        "object_path": FieldMetadata("object_path", "str", supports_text_search=True),
        "object_type": FieldMetadata("object_type", "str"),
        "size_bytes": FieldMetadata("size_bytes", "int"),
        "last_modified": FieldMetadata("last_modified", "datetime"),
        "last_modified_date": FieldMetadata("last_modified", "datetime"),
        "created_date": FieldMetadata("created_date", "datetime"),
        "discovery_timestamp": FieldMetadata("discovery_timestamp", "datetime"),
        "datasource_id": FieldMetadata("data_source_id", "str"),
    },
    
    "OBJECTMETADATA": {
        "object_id": FieldMetadata("object_key_hash", "bytes"),
        "object_path": FieldMetadata("object_path", "str", supports_text_search=True),
        "metadata_fetch_timestamp": FieldMetadata("metadata_fetch_timestamp", "datetime"),
        "datasource_id": FieldMetadata("data_source_id", "str"),
        # detailed_metadata fields would need special JSON path handling
    },
    
    "SCANFINDINGS": {
        "finding_id": FieldMetadata("finding_key_hash", "bytes"),
        "scan_job_id": FieldMetadata("scan_job_id", "str"),
        "datasource_id": FieldMetadata("data_source_id", "str"),
        "classifier_id": FieldMetadata("classifier_id", "str"),
        "entity_type": FieldMetadata("entity_type", "str"),
        "confidence_tier": FieldMetadata("confidence_tier", "str"),
        "file_path": FieldMetadata("file_path", "str", supports_text_search=True),
        "file_name": FieldMetadata("file_name", "str", supports_text_search=True),
        "file_extension": FieldMetadata("file_extension", "str"),
        "table_name": FieldMetadata("table_name", "str"),
        "field_name": FieldMetadata("field_name", "str"),
        "schema_name": FieldMetadata("schema_name", "str"),
        "finding_count": FieldMetadata("finding_count", "int"),
        "average_confidence": FieldMetadata("average_confidence", "float"),
        "max_confidence": FieldMetadata("max_confidence", "float"),
        "scan_timestamp": FieldMetadata("scan_timestamp", "datetime"),
        "classification_date": FieldMetadata("scan_timestamp", "datetime"),  # Alias
    },
    
    "POLICY_ACTION_RESULTS": {
        "object_id": FieldMetadata("object_key_hash", "bytes"),
        "object_path": FieldMetadata("object_path", "str", supports_text_search=True),
        "policy_id": FieldMetadata("policy_id", "str"),
        "policy_name": FieldMetadata("policy_name", "str"),
        "policy_status": FieldMetadata("status", "str"),  # Map to 'status'
        "action_status": FieldMetadata("status", "str"),   # Alias
        "status": FieldMetadata("status", "str"),
        "action_type": FieldMetadata("action_type", "str"),
        "policy_action_type": FieldMetadata("action_type", "str"),  # Alias
        "timestamp_utc": FieldMetadata("timestamp_utc", "datetime"),
        "action_date": FieldMetadata("timestamp_utc", "datetime"),  # Alias
        "policy_tags": FieldMetadata("policy_tags", "json_array"),
        "new_object_path": FieldMetadata("new_object_path", "str"),
        "policy_new_object_path": FieldMetadata("new_object_path", "str"),  # Alias
        "datasource_id": FieldMetadata("datasource_id", "str"),
        "job_id": FieldMetadata("job_id", "int"),
    }
}

# Policy-specific fields that require JOIN with POLICY_ACTION_RESULTS
POLICY_FIELDS = {
    "policy_id", "policy_name", "policy_status", "action_status", 
    "status", "action_type", "policy_action_type", "timestamp_utc", 
    "action_date", "policy_tags", "new_object_path", 
    "policy_new_object_path"
}

def get_field_metadata(
    data_source: DataSourceType, 
    field_name: str
) -> FieldMetadata:
    """Get field metadata with validation"""
    mappings = FIELD_MAPPINGS.get(data_source)
    if not mappings:
        raise ValueError(f"Unknown data source: {data_source}")
    
    metadata = mappings.get(field_name)
    if not metadata:
        raise ValueError(
            f"Unknown field '{field_name}' for data source '{data_source}'. "
            f"Valid fields: {', '.join(mappings.keys())}"
        )
    
    return metadata

def is_policy_field(field_name: str) -> bool:
    """Check if field requires JOIN with POLICY_ACTION_RESULTS"""
    return field_name in POLICY_FIELDS
