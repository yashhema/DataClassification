# src/core/db_models/system_parameters_schema.py
"""
Defines the database schema for System Parameters using the SQLAlchemy ORM.
This table allows for dynamic and centralized management of system-wide
and component-specific configurations.
"""

from typing import Optional
import enum

from sqlalchemy import (
    String, Integer, ForeignKey, Enum as SQLAlchemyEnum
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from .base import Base

class ComponentType(enum.Enum):
    """Defines the system component a parameter can apply to."""
    GLOBAL = "global"
    ORCHESTRATOR = "orchestrator"
    WORKER = "worker"
    CONNECTOR = "connector"

class SystemParameter(Base):
    __tablename__ = 'SystemParameters'
    __table_args__ = {'extend_existing': True}
    __doc__ = """
    Stores tunable system parameters. This table holds overrides to the
    default values defined in the system.yaml configuration file.

    Usage:
    On startup, the application loads defaults from system.yaml. It then
    queries this table to get any overrides. This allows for dynamic tuning
    of the running system without requiring a redeployment.
    """
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Example: "worker.task_timeout_seconds"
    # The key name of the parameter.
    parameter_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="The unique key name of the parameter.")

    # Example: "600"
    # The configured value for the parameter, stored as a string.
    parameter_value: Mapped[str] = mapped_column(String(255), nullable=False, comment="The value for the parameter.")

    # Example: "worker"
    # The component this parameter applies to. 'GLOBAL' applies to all.
    applicable_to: Mapped[ComponentType] = mapped_column(SQLAlchemyEnum(ComponentType), nullable=False, default=ComponentType.GLOBAL, comment="The component this parameter applies to.")

    description: Mapped[Optional[str]] = mapped_column(String(1024), comment="A human-readable description of the parameter's function.")

    # An optional link to a specific NodeGroup. If NULL, the parameter is global
    # for its 'ApplicableTo' type. If set, it's an override for a specific group of workers.
    node_group_id: Mapped[Optional[int]] = mapped_column(ForeignKey('node_groups.id'), comment="An optional link to a NodeGroup for component-specific overrides.")