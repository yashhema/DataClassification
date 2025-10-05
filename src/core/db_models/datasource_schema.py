# src/core/db_models/datasource_schema.py
"""
Defines the database schema for the DataSource and its related metadata
tables (Tags, NodeGroups) using the SQLAlchemy ORM.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, ForeignKey, JSON,Integer
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

from sqlalchemy.dialects.mssql import NVARCHAR,VARCHAR, DATETIMEOFFSET
from typing import TYPE_CHECKING
from .association_tables import DataSourceTagLink, DatasourceToOverrideGroupLink
if TYPE_CHECKING:
    from .calendar_schema import Calendar
    from .compliance_schemas import OverrideGroup  # ADDED
    # Import the Pydantic model for documentation purposes

class NodeGroup(Base):
    __tablename__ = 'node_groups'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))

class Tag(Base):
    __tablename__ = 'tags'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    tag_type: Mapped[str] = mapped_column(String(50))
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey('tags.id'))
    parent: Mapped["Tag"] = relationship(back_populates="children", remote_side=[id])
    children: Mapped[List["Tag"]] = relationship(back_populates="parent")
    metadata_json: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSON)
    datasources: Mapped[List["DataSource"]] = relationship(secondary=DataSourceTagLink, back_populates="tags")

class DataSource(Base):
    __tablename__ = 'datasources'
    __doc__ = "Stores the configuration and operational state for each data source the system can scan."

    datasource_id: Mapped[str] = mapped_column(NVARCHAR(255), primary_key=True)
    name: Mapped[str] = mapped_column(NVARCHAR(255), nullable=False)
    datasource_type: Mapped[str] = mapped_column(NVARCHAR(50), nullable=False)
    

    
    node_group: Mapped[Optional["NodeGroup"]] = relationship()
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    
    node_group_id: Mapped[Optional[int]] = mapped_column(ForeignKey('node_groups.id'))
    calendar_id: Mapped[Optional[int]] = mapped_column(ForeignKey('calendars.id'))
    calendar: Mapped[Optional["Calendar"]] = relationship()
    tags: Mapped[List["Tag"]] = relationship(secondary=DataSourceTagLink, back_populates="datasources")
    
    override_groups: Mapped[List["OverrideGroup"]] = relationship(
        "OverrideGroup",  # CHANGED: String reference to avoid circular import
        secondary=DatasourceToOverrideGroupLink,
        back_populates="datasources"
    )

class DataSourceMetadata(Base):
    __tablename__ = 'datasource_metadata'
    datasource_id: Mapped[str] = mapped_column(VARCHAR(255), ForeignKey('datasources.datasource_id'), primary_key=True)
    product_version: Mapped[Optional[str]] = mapped_column(NVARCHAR(255), comment="The discovered product version of the data source.")
    profile_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, comment="Version number, incremented on each detected change.")
    deployment_model: Mapped[Optional[str]] = mapped_column(NVARCHAR(50))
    capabilities: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    last_profiled_timestamp: Mapped[datetime] = mapped_column(DATETIMEOFFSET(7), nullable=False)
