# src/core/db_models/discovery_catalog_schema.py
"""
Defines the database schema for the Discovery Catalog, which stores persistent
records of discovered objects and their detailed metadata.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sqlalchemy import (
    String, Integer, LargeBinary, DateTime, JSON, Index, ForeignKey, Text, Boolean
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

class DiscoveredObject(Base):
    __tablename__ = 'discovered_objects'
    __table_args__ = (
        Index('uq_object_key_hash', 'object_key_hash', unique=True),
        Index('ix_discovered_objects_data_source_id', 'data_source_id'),
        Index('ix_discovered_objects_object_type', 'object_type'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores the essential, enumerated information for every object discovered.
    Uniqueness is enforced by a SHA-256 hash of the object's core identity
    (e.g., datasource + path + name) to ensure efficiency and avoid key length limits.
    """
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # A SHA-256 hash (32 bytes) of the object's unique identifying fields.
    object_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)

    # Core identity and location fields
    data_source_id: Mapped[str] = mapped_column(String(255), nullable=False)
    object_type: Mapped[str] = mapped_column(String(50), nullable=False)
    object_path: Mapped[str] = mapped_column(String(4000), nullable=False)
    
    # Basic metadata from enumeration
    size_bytes: Mapped[Optional[int]] = mapped_column(Integer)
    created_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_modified: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_accessed: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # System timestamp for when this object was first discovered
    discovery_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )
    
    # Establishes the one-to-one relationship with the metadata and classification info tables
    object_metadata: Mapped[Optional["ObjectMetadata"]] = relationship(back_populates="discovered_object")
    classification_info: Mapped[Optional["DiscoveredObjectClassificationDateInfo"]] = relationship(back_populates="discovered_object")


class ObjectMetadata(Base):
    __tablename__ = 'object_metadata'
    __doc__ = """
    Stores the detailed, rich metadata for a discovered object, fetched during
    the second phase of discovery. This table has a one-to-one relationship
    with the DiscoveredObjects table.
    """
    # This column is both the Primary Key and the Foreign Key,
    # enforcing a strict one-to-one relationship with a DiscoveredObject.
    object_id: Mapped[int] = mapped_column(ForeignKey('discovered_objects.id', ondelete="CASCADE"), primary_key=True)

    # A single JSON column to flexibly store type-specific metadata
    # (e.g., file permissions, table schema, field statistics).
    detailed_metadata: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

    # System timestamp for when the detailed metadata was last fetched or updated
    metadata_fetch_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )

    # Establishes the back-reference for the one-to-one relationship
    discovered_object: Mapped["DiscoveredObject"] = relationship(back_populates="metadata")


class DiscoveredObjectClassificationDateInfo(Base):
    __tablename__ = 'discovered_object_classification_date_info'
    __doc__ = """
    Stores the last classification timestamp for a discovered object.
    This allows the system to track which objects need to be re-classified.
    It has a one-to-one relationship with the DiscoveredObjects table.
    """
    # This column is both the Primary Key and the Foreign Key,
    # enforcing a strict one-to-one relationship with a DiscoveredObject.
    object_id: Mapped[int] = mapped_column(ForeignKey('discovered_objects.id', ondelete="CASCADE"), primary_key=True)

    # The timestamp of the last successful classification scan for this object.
    last_classification_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )

    # Establishes the back-reference for the one-to-one relationship
    discovered_object: Mapped["DiscoveredObject"] = relationship(back_populates="classification_info")