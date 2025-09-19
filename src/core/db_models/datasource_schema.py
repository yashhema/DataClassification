# src/core/db_models/datasource_schema.py
"""
Defines the database schema for the DataSource and its related metadata
tables (Tags, NodeGroups) using the SQLAlchemy ORM.
"""

from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, ForeignKey, JSON, Table, Column
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
from .association_tables import DataSourceTagLink

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .calendar_schema import Calendar
    # Import the Pydantic model for documentation purposes
    from core.config.config_models import DataSourceConfiguration

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

    datasource_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    datasource_type: Mapped[str] = mapped_column(String(50), nullable=False)
    node_group: Mapped[Optional["NodeGroup"]] = relationship()
    # UPDATED COMMENT: This now references the Pydantic model for its structure.
    configuration: Mapped[Dict[str, Any]] = mapped_column(
        JSON, 
        nullable=False,
        comment="JSON object containing all connection details and scan profiles. The structure is defined by the 'DataSourceConfiguration' Pydantic model in core/config/config_models.py."
    )
    
    node_group_id: Mapped[Optional[int]] = mapped_column(ForeignKey('node_groups.id'))
    calendar_id: Mapped[Optional[int]] = mapped_column(ForeignKey('calendars.id'))
    calendar: Mapped[Optional["Calendar"]] = relationship()
    tags: Mapped[List["Tag"]] = relationship(secondary=DataSourceTagLink, back_populates="datasources")

