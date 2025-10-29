# src/core/db_models/component_registry_schema.py
"""
Schema for tracking all deployed components (workers, orchestrators).
Used for fleet management and operations.
"""
from sqlalchemy import String, JSON, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from typing import Dict, Any, Optional
from .base import Base

class ComponentRegistry(Base):
    """
    Registry of all active system components for monitoring and management.
    
    This table tracks workers, orchestrators, and backend instances across
    all datacenters for operational visibility.
    """
    __tablename__ = 'component_registry'
    
    component_id: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
        comment="Unique component identifier (e.g., worker-abc-123)"
    )
    component_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        comment="Type: 'worker', 'orchestrator', 'backend'"
    )
    node_group: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        comment="Node group this component belongs to"
    )
    version: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        comment="Software version (e.g., 2.5.0)"
    )
    capabilities: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True,
        comment="JSON with supported features, connectors, etc."
    )
    last_heartbeat: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="Last time component reported status"
    )
    registered_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        comment="When component first registered"
    )
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="active",
        comment="Status: 'active', 'draining', 'offline'"
    )
    worker_type: Mapped[str] = mapped_column(String(100), nullable=False)
