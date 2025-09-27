import hashlib
from typing import Dict, Any
import uuid

# This new function should be added to the hash_utils.py file
def generate_task_id() -> bytes:
    """
    Generates a new, unique, random task ID using UUIDv4.

    Returns:
        A 16-byte UUID, which is compatible with the VARBINARY(32) database
        column (as it's less than 32 bytes). This provides a simple and
        highly reliable method for generating unique task identifiers on the
        application side.
    """
    # uuid.uuid4() creates a random UUID.
    # .bytes returns the 16-byte representation of the UUID.
    return uuid.uuid4().bytes

def generate_object_key_hash(datasource_id: str, object_path: str, object_type: str) -> bytes:
    """
    Generates the canonical SHA-256 hash for a discovered object.
    This is the primary key for the discovered_objects and object_metadata tables.
    The key is based on WHAT the object is, not WHEN it was found.
    """
    try:
        key_string = f"{datasource_id}|{object_path}|{object_type}"
        return hashlib.sha256(key_string.encode('utf-8')).digest()
    except UnicodeEncodeError as e:
        # Adds error handling for cases where paths may have encoding issues
        raise ValueError(f"Failed to encode object key components for hashing: {e}")

def generate_finding_key_hash(finding_context: Dict[str, Any]) -> bytes:
    """
    Generates the canonical SHA-256 hash for a scan finding summary.
    This is the primary key for the scan_finding_summaries table.
    The key is based on WHAT was found and WHERE, not WHEN it was found.
    """
    try:
        # CORRECTED: scan_job_id is REMOVED from the key components.
        key_components = [
            finding_context.get('data_source_id', ''),
            finding_context.get('classifier_id', ''),
            finding_context.get('entity_type', ''),
            finding_context.get('schema_name', '') or '',
            finding_context.get('table_name', '') or '',
            finding_context.get('field_name', '') or '',
            finding_context.get('file_path', '') or '',
            finding_context.get('file_name', '') or ''
        ]
        key_string = '|'.join(str(comp) for comp in key_components)
        return hashlib.sha256(key_string.encode('utf-8')).digest()
    except UnicodeEncodeError as e:
        raise ValueError(f"Failed to encode finding key components for hashing: {e}")
