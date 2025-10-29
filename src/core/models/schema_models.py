from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class ColumnInfo(BaseModel):
    """Represents a single column in a table schema"""
    name: str
    data_type: str
    nullable: bool = True
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

class SchemaFingerprint(BaseModel):
    """Snapshot of a table's schema at a point in time"""
    table_name: str
    columns: List[ColumnInfo]
    captured_at: str  # ISO timestamp
    
    def to_hash(self) -> str:
        """Generate hash for schema comparison"""
        import hashlib
        import json
        schema_str = json.dumps({
            "columns": [(c.name, c.data_type) for c in self.columns]
        }, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

class SchemaComparisonResult(BaseModel):
    """Result of comparing two schemas"""
    has_changes: bool
    added_columns: List[str] = []
    removed_columns: List[str] = []
    modified_columns: List[str] = []
    
def compare_schemas(
    old: SchemaFingerprint, 
    new: SchemaFingerprint
) -> SchemaComparisonResult:
    """MVP schema comparison: column name and type changes only"""
    old_cols = {c.name: c.data_type for c in old.columns}
    new_cols = {c.name: c.data_type for c in new.columns}
    
    added = set(new_cols.keys()) - set(old_cols.keys())
    removed = set(old_cols.keys()) - set(new_cols.keys())
    
    modified = [
        name for name in (old_cols.keys() & new_cols.keys())
        if old_cols[name] != new_cols[name]
    ]
    
    return SchemaComparisonResult(
        has_changes=bool(added or removed or modified),
        added_columns=list(added),
        removed_columns=list(removed),
        modified_columns=modified
    )