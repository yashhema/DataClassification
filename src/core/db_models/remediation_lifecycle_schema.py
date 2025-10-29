# Intended for a new file, e.g., core/db_models/remediation_lifecycle_schema.py

import enum
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, Enum as SQLAlchemyEnum,
    UniqueConstraint, Index, Text, LargeBinary, func, CheckConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
# Assuming PostgreSQL/Yugabyte compatibility with TIMESTAMPTZ and JSONB
from sqlalchemy.dialects.postgresql import JSONB

from .base import Base
# Assuming association table is defined correctly elsewhere if needed for ListOfDataSources approach
# from .association_tables import RemediationGroupDataSourceLink

# --- Enums for Status Fields ---

class RemediationGroupStatus(str, enum.Enum):
    OPEN = "Open"
    CLOSED = "Closed"
    ACCEPTABLY_CLOSED = "AcceptablyClosed" # Added based on policy conditions

class AuditorApprovedStatus(str, enum.Enum):
    PENDING = "Pending"
    APPROVED_ALL_CLOSED = "ApprovedAllClosed"
    APPROVED_PARTIALLY_CLOSED = "ApprovedPartiallyClosed"
    FORCED_CLOSED_FOR_VERSION_UPGRADE = "ForcedClosedForVersionUpgrade"

class RemediationDataSourceStatus(str, enum.Enum):
    NONE = "None" # Initial state before group opens
    OPEN = "Open"
    ACKNOWLEDGED = "Acknowledged" # User acknowledged all relevant tables
    REQUEST_FOR_SCAN = "RequestForScan"
    CLOSED = "Closed"
    ACCEPTABLY_CLOSED = "AcceptablyClosed" # Added based on policy conditions

class RemediationTableStatusEnum(str, enum.Enum):
    PENDING = "Pending"
    ACKNOWLEDGED = "Acknowledged"

class RemediationObjectStatus(str, enum.Enum):
    ACCEPTED = "Accepted"
    FALSE_POSITIVE = "FalsePositive"

# --- ORM Models ---

class RemediationLifeCycleGroup(Base):
    __tablename__ = 'remediation_lifecycle_group'
    __table_args__ = (
        UniqueConstraint('GroupName', 'CurrentVersion', name='uq_remediation_group_version'),
        {'comment': 'Tracks high-level remediation initiatives driven by policies.'}
    )

    GroupName: Mapped[str] = mapped_column(String(255), primary_key=True)
    CurrentVersion: Mapped[int] = mapped_column(Integer, primary_key=True)
    PolicyName: Mapped[str] = mapped_column(String(255), nullable=False, comment="Policy Template Name or ID driving this.")
    CurrentStatus: Mapped[RemediationGroupStatus] = mapped_column(SQLAlchemyEnum(RemediationGroupStatus, name='remediation_group_status_enum'), nullable=False)
    LastUpdatedBy: Mapped[Optional[str]] = mapped_column(String(255))
    LastUpdateDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))
    Comments: Mapped[Optional[str]] = mapped_column(Text)
    StatusHistory: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB) # Array of {status, timestamp, user}
    ProgressIndicators: Mapped[Optional[Dict[str, int]]] = mapped_column(JSONB, comment="Dict: {CountOpen: X, CountAcknowledged: Y, ...}")
    StartDate: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    EndDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    AuditorApprovedStatus: Mapped[AuditorApprovedStatus] = mapped_column(SQLAlchemyEnum(AuditorApprovedStatus, name='auditor_approved_status_enum'), nullable=False, default=AuditorApprovedStatus.PENDING)
    AuditorNotes: Mapped[Optional[str]] = mapped_column(Text)
    AuditedBy: Mapped[Optional[str]] = mapped_column(String(255))
    AuditedDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    # Relationship to link table
    datasources_link: Mapped[List["RemediationGroupDataSourceLink"]] = relationship(back_populates="group", cascade="all, delete-orphan")

class RemediationGroupDataSourceLink(Base):
    __tablename__ = 'remediation_group_datasource_link'
    __table_args__ = (
        UniqueConstraint('GroupName', 'CurrentVersion', 'DataSourceName', name='uq_remediation_group_ds_link'),
        ForeignKeyConstraint(['GroupName', 'CurrentVersion'],
                             ['remediation_lifecycle_group.GroupName', 'remediation_lifecycle_group.CurrentVersion'],
                             ondelete="CASCADE"),
        # Optional FK to DataSources table:
        # ForeignKeyConstraint(['DataSourceName'], ['datasources.datasource_id']),
        {'comment': 'Links DataSources to a specific remediation group version.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True) # Simple surrogate key
    GroupName: Mapped[str] = mapped_column(String(255), nullable=False)
    CurrentVersion: Mapped[int] = mapped_column(Integer, nullable=False)
    DataSourceName: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    # Relationship back to the group
    group: Mapped["RemediationLifeCycleGroup"] = relationship(back_populates="datasources_link")


class RemediationLifeCycleDataSource(Base):
    __tablename__ = 'remediation_lifecycle_datasource'
    __table_args__ = (
        UniqueConstraint('GroupName', 'CurrentVersion', 'DataSourceName', name='uq_remediation_datasource_version'),
        ForeignKeyConstraint(['GroupName', 'CurrentVersion'],
                             ['remediation_lifecycle_group.GroupName', 'remediation_lifecycle_group.CurrentVersion'],
                             ondelete="CASCADE"),
        Index('ix_remediation_ds_status', 'Status'),
        {'comment': 'Tracks status for each DataSource within a Remediation Group/Version.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True) # Simple surrogate key
    GroupName: Mapped[str] = mapped_column(String(255), nullable=False)
    DataSourceName: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    CurrentVersion: Mapped[int] = mapped_column(Integer, nullable=False)
    Excluded: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    ExcludedReason: Mapped[Optional[str]] = mapped_column(Text)
    ExclusionApprovedBy: Mapped[Optional[str]] = mapped_column(String(255))
    ExclusionTableslist: Mapped[Optional[List[str]]] = mapped_column(JSONB) # List of excluded tables/objects
    Status: Mapped[RemediationDataSourceStatus] = mapped_column(SQLAlchemyEnum(RemediationDataSourceStatus, name='remediation_ds_status_enum'), nullable=False, default=RemediationDataSourceStatus.NONE)
    ProgressIndicators: Mapped[Optional[Dict[str, int]]] = mapped_column(JSONB, comment="Dict: {CountTablesPending: X, CountTablesAcknowledged: Y}")
    StartDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    LastUpdatedBy: Mapped[Optional[str]] = mapped_column(String(255))
    LastUpdateDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))
    StatusHistory: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB)
    ExclusionApprovalHistory: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB)

    # Relationship to table statuses
    table_statuses: Mapped[List["RemediationLifeCycleTableStatus"]] = relationship(back_populates="datasource_cycle", cascade="all, delete-orphan")


class RemediationLifeCycleTableStatus(Base):
    __tablename__ = 'remediation_lifecycle_table_status'
    __table_args__ = (
        UniqueConstraint('GroupName', 'CurrentVersion', 'DataSourceName', 'SchemaName', 'TableName', name='uq_remediation_table_status_key'),
        ForeignKeyConstraint(['GroupName', 'CurrentVersion', 'DataSourceName'],
                             ['remediation_lifecycle_datasource.GroupName', 'remediation_lifecycle_datasource.CurrentVersion', 'remediation_lifecycle_datasource.DataSourceName'],
                             ondelete="CASCADE"),
        Index('ix_remediation_table_ds_lookup', 'GroupName', 'CurrentVersion', 'DataSourceName'),
        {'comment': 'Tracks user acknowledgment status per Table within a DataSource/Group/Version.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True) # Simple surrogate key
    GroupName: Mapped[str] = mapped_column(String(255), nullable=False)
    CurrentVersion: Mapped[int] = mapped_column(Integer, nullable=False)
    DataSourceName: Mapped[str] = mapped_column(String(255), nullable=False)
    SchemaName: Mapped[str] = mapped_column(String(255), nullable=False)
    TableName: Mapped[str] = mapped_column(String(255), nullable=False)
    Status: Mapped[RemediationTableStatusEnum] = mapped_column(SQLAlchemyEnum(RemediationTableStatusEnum, name='remediation_table_status_enum'), nullable=False, default=RemediationTableStatusEnum.PENDING)
    AcknowledgedBy: Mapped[Optional[str]] = mapped_column(String(255))
    AcknowledgedDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    LastUpdatedBy: Mapped[Optional[str]] = mapped_column(String(255))
    LastUpdateDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))

    # Relationship back to datasource cycle
    datasource_cycle: Mapped["RemediationLifeCycleDataSource"] = relationship(back_populates="table_statuses")


class RemediationLifeCycleObject(Base):
    __tablename__ = 'remediation_lifecycle_object'
    __table_args__ = (
        UniqueConstraint('GroupName', 'CurrentVersion', 'finding_key_hash', name='uq_remediation_object_key'),
        ForeignKeyConstraint(['GroupName', 'CurrentVersion'],
                             ['remediation_lifecycle_group.GroupName', 'remediation_lifecycle_group.CurrentVersion'],
                             ondelete="CASCADE"),
        Index('ix_remediation_object_finding_key', 'finding_key_hash'),
        Index('ix_remediation_object_cycle', 'GroupName', 'CurrentVersion'),
        {'comment': 'Tracks status of individual findings relevant to a remediation cycle.'}
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True) # Simple surrogate key
    GroupName: Mapped[str] = mapped_column(String(255), nullable=False)
    CurrentVersion: Mapped[int] = mapped_column(Integer, nullable=False)
    finding_key_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False, comment="SHA256 hash from ScanFindingSummary")
    Status: Mapped[RemediationObjectStatus] = mapped_column(SQLAlchemyEnum(RemediationObjectStatus, name='remediation_object_status_enum'), nullable=False, default=RemediationObjectStatus.ACCEPTED)
    LastUpdatedBy: Mapped[Optional[str]] = mapped_column(String(255))
    LastUpdateDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))
    StatusHistory: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(JSONB)