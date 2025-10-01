# src/core/db_models/discovery_catalog_schema.py
"""
Defines the database schema for the Discovery Catalog, which stores persistent
records of discovered objects and their detailed metadata.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sqlalchemy import (
    String, Integer, LargeBinary, DateTime, JSON, Index
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

class DiscoveredObject(Base):
    __tablename__ = 'discovered_objects'
    __table_args__ = (
        Index('ix_discovered_objects_data_source_id', 'data_source_id'),
        Index('ix_discovered_objects_object_type', 'object_type'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores the essential, enumerated information for every object discovered.
    Uniqueness is enforced by a SHA-256 hash of the object's core identity
    (e.g., datasource + path + name) to ensure efficiency and avoid key length limits.
    """

    # A SHA-256 hash (32 bytes) of the object's unique identifying fields - now PRIMARY KEY
    object_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)

    # Core identity and location fields
    data_source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    object_type: Mapped[str] = mapped_column(String(50), nullable=False)
    object_path: Mapped[str] = mapped_column(String(4000), nullable=False)
    
    # Basic metadata from enumeration
    size_bytes: Mapped[Optional[int]] = mapped_column(Integer)
    created_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_modified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_accessed: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # System timestamp for when this object was first discovered
    discovery_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )


class ObjectMetadata(Base):
    __tablename__ = 'object_metadata'
    __doc__ = """
    Stores the detailed, rich metadata for a discovered object, fetched during
    the second phase of discovery. This table has a one-to-one relationship
    with the DiscoveredObjects table.
    """
    # Using object_key_hash as primary key instead of foreign key relationship
    object_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)

    # A single JSON column to flexibly store type-specific metadata
    # (e.g., file permissions, table schema, field statistics).
    detailed_metadata: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

    # A single JSON column to flexibly store type-specific discovered object
    # (e.g., file permissions, table schema, field statistics).
    base_object: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=True)

    # System timestamp for when the detailed metadata was last fetched or updated
    metadata_fetch_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )


class DiscoveredObjectClassificationDateInfo(Base):
    __tablename__ = 'discovered_object_classification_date_info'
    __doc__ = """
    Stores the last classification timestamp for a discovered object.
    This allows the system to track which objects need to be re-classified.
    """
    # Using object_key_hash as primary key instead of foreign key relationship
    object_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)

    # The timestamp of the last successful classification scan for this object.
    last_classification_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )