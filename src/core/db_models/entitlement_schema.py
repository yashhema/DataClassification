#  core/db_models/entitlement_schema.py
"""
Defines the database schema for storing normalized, cross-platform
entitlement snapshots.
"""
from .base import Base
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, Enum as SQLAlchemyEnum,
    UniqueConstraint, Text
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.mssql import NVARCHAR, DATETIMEOFFSET

class EntitlementSnapshot(Base):
    __tablename__ = 'entitlement_snapshots'
    __table_args__ = (
        UniqueConstraint('datasource_id', 'cycle_id', name='uq_entitlement_snapshot_cycle'),
    )
    snapshot_id: Mapped[int] = mapped_column(primary_key=True)
    datasource_id: Mapped[str] = mapped_column(NVARCHAR(255), nullable=False)
    cycle_id: Mapped[str] = mapped_column(NVARCHAR(255), nullable=False)
    snapshot_timestamp: Mapped[datetime] = mapped_column(DATETIMEOFFSET(7), nullable=False)
    snapshot_status: Mapped[str] = mapped_column(SQLAlchemyEnum('LOADING', 'ACTIVE', 'SUPERSEDED', 'FAILED', name='snapshot_status_enum'), nullable=False)

    # Relationships to child tables for cascading deletes
    principals: Mapped[List["EntitlementPrincipal"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    memberships: Mapped[List["EntitlementMembership"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    permissions: Mapped[List["EntitlementPermission"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    raw_data: Mapped[Optional["RawEntitlementData"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")

class EntitlementPrincipal(Base):
    __tablename__ = 'entitlement_principals'
    id: Mapped[int] = mapped_column(primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey('entitlement_snapshots.snapshot_id'), nullable=False, index=True)
    principal_name: Mapped[str] = mapped_column(NVARCHAR(512), nullable=False)
    principal_type: Mapped[str] = mapped_column(SQLAlchemyEnum('USER', 'ROLE', 'GROUP', 'LOGIN', name='principal_type_enum'), nullable=False)
    authentication_source: Mapped[str] = mapped_column(SQLAlchemyEnum('LOCAL', 'AD', 'LDAP', 'IAM', 'AZURE_AD', 'SSO', name='auth_source_enum'), nullable=False)
    is_enabled: Mapped[Optional[bool]] = mapped_column(Boolean)
    principal_attributes: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    snapshot: Mapped["EntitlementSnapshot"] = relationship(back_populates="principals")

class EntitlementMembership(Base):
    __tablename__ = 'entitlement_memberships'
    id: Mapped[int] = mapped_column(primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey('entitlement_snapshots.snapshot_id'), nullable=False, index=True)
    member_principal_name: Mapped[str] = mapped_column(NVARCHAR(512), nullable=False)
    container_principal_name: Mapped[str] = mapped_column(NVARCHAR(512), nullable=False)
    snapshot: Mapped["EntitlementSnapshot"] = relationship(back_populates="memberships")

class EntitlementPermission(Base):
    __tablename__ = 'entitlement_permissions'
    id: Mapped[int] = mapped_column(primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey('entitlement_snapshots.snapshot_id'), nullable=False, index=True)
    principal_name: Mapped[str] = mapped_column(NVARCHAR(512), nullable=False)
    securable_type: Mapped[str] = mapped_column(NVARCHAR(100), nullable=False)
    securable_path: Mapped[str] = mapped_column(NVARCHAR(4000), nullable=False)
    permission_type: Mapped[str] = mapped_column(NVARCHAR(100), nullable=False)
    grant_type: Mapped[str] = mapped_column(SQLAlchemyEnum('GRANT', 'DENY', name='grant_type_enum'), nullable=False)
    is_inherited: Mapped[bool] = mapped_column(Boolean, nullable=False)
    vendor_permission_name: Mapped[Optional[str]] = mapped_column(NVARCHAR(255))
    snapshot: Mapped["EntitlementSnapshot"] = relationship(back_populates="permissions")

class RawEntitlementData(Base):
    __tablename__ = 'raw_entitlement_data'
    snapshot_id: Mapped[int] = mapped_column(ForeignKey('entitlement_snapshots.snapshot_id'), primary_key=True)
    raw_data_json: Mapped[str] = mapped_column(NVARCHAR(None), nullable=False) # nvarchar(max)
    snapshot: Mapped["EntitlementSnapshot"] = relationship(back_populates="raw_data")