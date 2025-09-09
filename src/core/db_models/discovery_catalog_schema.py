# src/core/db_models/discovery_catalog_schema.py
"""
Defines the database schema for the Discovery Catalog, which stores persistent
records of discovered objects and their detailed metadata.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sqlalchemy import (
    String, Integer, LargeBinary, DateTime, JSON, Index, ForeignKey
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from .base import Base

class DiscoveredObject(Base):
    __tablename__ = 'DiscoveredObjects'
    __table_args__ = (
        Index('uq_object_key_hash', 'ObjectKeyHash', unique=True),
        Index('IX_DiscoveredObjects_DataSourceID', 'DataSourceID'),
        Index('IX_DiscoveredObjects_ObjectType', 'ObjectType'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores the essential, enumerated information for every object discovered.
    Uniqueness is enforced by a SHA-256 hash of the object's core identity
    (e.g., datasource + path + name) to ensure efficiency and avoid key length limits.
    """
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # A SHA-256 hash (32 bytes) of the object's unique identifying fields.
    ObjectKeyHash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)

    # Core identity and location fields
    DataSourceID: Mapped[str] = mapped_column(String(255), nullable=False)
    ObjectType: Mapped[str] = mapped_column(String(50), nullable=False)
    ObjectPath: Mapped[str] = mapped_column(String(4000), nullable=False)
    
    # Basic metadata from enumeration
    SizeBytes: Mapped[Optional[int]] = mapped_column(Integer)
    CreatedDate: Mapped[Optional[datetime]] = mapped_column(DateTime)
    LastModified: Mapped[Optional[datetime]] = mapped_column(DateTime)
    LastAccessed: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # System timestamp for when this object was first discovered
    DiscoveryTimestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )
    
    # Establishes the one-to-one relationship with the metadata and classification info tables
    Metadata: Mapped["ObjectMetadata"] = relationship(back_populates="DiscoveredObject")
    ClassificationInfo: Mapped["DiscoveredObjectClassificationDateInfo"] = relationship(back_populates="DiscoveredObject")


class ObjectMetadata(Base):
    __tablename__ = 'ObjectMetadata'
    __table_args__ = (
        Index('ix_objectmetadata_datasource_id', 'DataSourceID'),
        Index('ix_objectmetadata_filename', 'FileName'),
        
        # NEW: Added indexes for commonly queried date fields
        Index('ix_objectmetadata_last_modified', 'LastModified'),
        Index('ix_objectmetadata_last_accessed', 'LastAccessed'),
        Index('ix_objectmetadata_last_scanned', 'LastScannedDate'),

        {'extend_existing': True}
    )
    __doc__ = """
    Stores the primary, long-term master record for a discovered object,
    including its key queryable properties, detailed metadata, and action history.
    """
    
    # The primary key remains the fixed-length hash of the object's identity.
    ObjectKeyHash: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)

    # --- Core Identity Fields (Promoted for Querying) ---
    DataSourceID: Mapped[str] = mapped_column(String(255), nullable=False)
    ObjectPath: Mapped[str] = mapped_column(Text, nullable=False)
    
    SchemaName: Mapped[Optional[str]] = mapped_column(String(255))
    TableName: Mapped[Optional[str]] = mapped_column(String(255))
    FieldName: Mapped[Optional[str]] = mapped_column(String(255))
    FileName: Mapped[Optional[str]] = mapped_column(String(255))

    # --- Basic Metadata (Promoted for Querying) ---
    SizeBytes: Mapped[Optional[int]] = mapped_column(Integer)
    CreatedDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    LastModified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    LastAccessed: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # --- System State and Remediation Tracking Fields ---
    LastScannedDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    RemovedDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    TombstoneFileName: Mapped[Optional[str]] = mapped_column(String(255))
    MovedPath: Mapped[Optional[str]] = mapped_column(Text)
    IsAvailable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    IsEncrypted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    
    # --- Flexible/Detailed Data Fields ---
    DetailedMetadata: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    ActionHistory: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSON,
        nullable=False,
        server_default="[]"
    )
    __tablename__ = 'ObjectMetadata'
    __doc__ = """
    Stores the detailed, rich metadata for a discovered object, fetched during
    the second phase of discovery. This table has a one-to-one relationship
    with the DiscoveredObjects table.
    """
    # This column is both the Primary Key and the Foreign Key,
    # enforcing a strict one-to-one relationship with a DiscoveredObject.
    ObjectID: Mapped[int] = mapped_column(ForeignKey('DiscoveredObjects.ID', ondelete="CASCADE"), primary_key=True)

    # A single JSON column to flexibly store type-specific metadata
    # (e.g., file permissions, table schema, field statistics).
    DetailedMetadata: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

    # System timestamp for when the detailed metadata was last fetched or updated
    MetadataFetchTimestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )

    # Establishes the back-reference for the one-to-one relationship
    DiscoveredObject: Mapped["DiscoveredObject"] = relationship(back_populates="Metadata")


class DiscoveredObjectClassificationDateInfo(Base):
    __tablename__ = 'DiscoveredObjectClassificationDateInfo'
    __doc__ = """
    Stores the last classification timestamp for a discovered object.
    This allows the system to track which objects need to be re-classified.
    It has a one-to-one relationship with the DiscoveredObjects table.
    """
    # This column is both the Primary Key and the Foreign Key,
    # enforcing a strict one-to-one relationship with a DiscoveredObject.
    ObjectID: Mapped[int] = mapped_column(ForeignKey('DiscoveredObjects.ID', ondelete="CASCADE"), primary_key=True)

    # The timestamp of the last successful classification scan for this object.
    LastClassificationDate: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False, 
        default=lambda: datetime.now(timezone.utc)
    )

    # Establishes the back-reference for the one-to-one relationship
    DiscoveredObject: Mapped["DiscoveredObject"] = relationship(back_populates="ClassificationInfo")
