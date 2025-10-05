# In core/db_models/compliance_schemas.py
from datetime import datetime
from .base import Base
from typing import List, Optional, Dict, Any
from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, Enum as SQLAlchemyEnum,
    UniqueConstraint, Text
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.mssql import NVARCHAR, DATETIMEOFFSET
from .association_tables import DatasourceToOverrideGroupLink
from typing import TYPE_CHECKING  # ADDED
if TYPE_CHECKING:
    from .datasource_schema import DataSource  # ADDED
class QuerySet(Base):
    __tablename__ = 'query_sets'
    id: Mapped[int] = mapped_column(primary_key=True)
    query_set_name: Mapped[str] = mapped_column(NVARCHAR(255))
    release_version: Mapped[str] = mapped_column(NVARCHAR(50))
    target_product: Mapped[str] = mapped_column(NVARCHAR(100))
    version_match_regex: Mapped[Optional[str]] = mapped_column(NVARCHAR(255))
    query_type: Mapped[str] = mapped_column(NVARCHAR(50))
    queries: Mapped[List["Query"]] = relationship(back_populates="query_set")

class Query(Base):
    __tablename__ = 'queries'
    id: Mapped[int] = mapped_column(primary_key=True)
    query_set_id: Mapped[int] = mapped_column(ForeignKey('query_sets.id'))
    check_id: Mapped[str] = mapped_column(NVARCHAR(100))
    query_text: Mapped[str] = mapped_column(NVARCHAR(None)) # nvarchar(max)
    timeout_seconds: Mapped[int] = mapped_column(default=300)
    version: Mapped[int] = mapped_column(default=1)
    last_updated_at: Mapped[Optional[datetime]] = mapped_column(DATETIMEOFFSET(7))
    last_updated_by: Mapped[Optional[str]] = mapped_column(NVARCHAR(255))
    query_set: Mapped["QuerySet"] = relationship(back_populates="queries")

class OverrideGroup(Base):
    __tablename__ = 'override_groups'
    id: Mapped[int] = mapped_column(primary_key=True)
    group_name: Mapped[str] = mapped_column(NVARCHAR(255), unique=True)
    group_type: Mapped[str] = mapped_column(SQLAlchemyEnum('CORPORATE', 'BUSINESS', 'APPLICATION', 'INDIVIDUAL', name='override_group_type_enum'))
    effective_date: Mapped[datetime] = mapped_column(DATETIMEOFFSET(7))
    
    datasources: Mapped[List["DataSource"]] = relationship(
        "DataSource",  # CHANGED: String reference to avoid circular import
        secondary=DatasourceToOverrideGroupLink,
        back_populates="override_groups"
    )
    

class OverrideRule(Base):
    __tablename__ = 'override_rules'
    id: Mapped[int] = mapped_column(primary_key=True)
    override_group_id: Mapped[int] = mapped_column(ForeignKey('override_groups.id'))
    check_id: Mapped[str] = mapped_column(NVARCHAR(100))
    override_type: Mapped[str] = mapped_column(SQLAlchemyEnum('EXCEPTION', 'CUSTOM_QUERY', name='override_type_enum'))
    exception_status: Mapped[Optional[str]] = mapped_column(SQLAlchemyEnum('NOT_APPLICABLE', 'RISK_ACCEPTED', name='exception_status_enum'))
    justification: Mapped[Optional[str]] = mapped_column(NVARCHAR(None)) # nvarchar(max)
    custom_query_text: Mapped[Optional[str]] = mapped_column(NVARCHAR(None)) # nvarchar(max)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    expiration_date: Mapped[Optional[datetime]] = mapped_column(DATETIMEOFFSET(7))

class BenchmarkFinding(Base):
    __tablename__ = 'benchmark_findings'
    datasource_id: Mapped[str] = mapped_column(VARCHAR(255), primary_key=True)
    check_id: Mapped[str] = mapped_column(NVARCHAR(100), primary_key=True)
    cycle_id: Mapped[str] = mapped_column(NVARCHAR(255), primary_key=True)
    status: Mapped[str] = mapped_column(NVARCHAR(50))
    job_execution_id: Mapped[Optional[str]] = mapped_column(NVARCHAR(255))
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    
class SensitiveDataLocation(Base):
    __tablename__ = 'sensitive_data_locations'
    datasource_id: Mapped[str] = mapped_column(NVARCHAR(255), primary_key=True)
    table_name: Mapped[str] = mapped_column(NVARCHAR(255), primary_key=True)
    classifications_found: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    last_updated_timestamp: Mapped[Optional[datetime]] = mapped_column(DATETIMEOFFSET(7))

class ScanCycle(Base):
    __tablename__ = 'scan_cycles'
    cycle_id: Mapped[str] = mapped_column(NVARCHAR(255), primary_key=True)
    cycle_name: Mapped[str] = mapped_column(NVARCHAR(255), nullable=False)
    cycle_type: Mapped[str] = mapped_column(SQLAlchemyEnum('BENCHMARK', 'VULNERABILITY', 'ENTITLEMENT', name='cycle_type_enum'))
    status: Mapped[str] = mapped_column(SQLAlchemyEnum('ACTIVE', 'CLOSING', 'CLOSED', 'ARCHIVED', name='cycle_status_enum'))
    start_date: Mapped[datetime] = mapped_column(DATETIMEOFFSET(7), nullable=False)
    end_date: Mapped[Optional[datetime]] = mapped_column(DATETIMEOFFSET(7))
    created_by: Mapped[Optional[str]] = mapped_column(NVARCHAR(255))
    closed_timestamp: Mapped[Optional[datetime]] = mapped_column(DATETIMEOFFSET(7))