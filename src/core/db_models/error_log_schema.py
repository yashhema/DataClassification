# src/core/db_models/error_log_schema.py
"""
Defines the SQLAlchemy ORM model for persisting critical system errors
to the database for auditing and post-mortem analysis.
"""

from datetime import datetime
from typing import Dict, Any, Optional

from sqlalchemy import String, Integer, DateTime, JSON, Text
from sqlalchemy.orm import Mapped, mapped_column

# Import the ORM base from a shared location
from .base import Base

class SystemErrorLog(Base):
    """SQLAlchemy ORM model for the SystemErrorLogs table."""
    __tablename__ = 'SystemErrorLogs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    error_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    error_type: Mapped[str] = mapped_column(String(100), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(50), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    
    # === NEW: Source and Origin Fields ===
    source_component: Mapped[Optional[str]] = mapped_column(String(100), comment="The name of the component that generated the error (e.g., 'Orchestrator', 'Worker').")
    source_machine: Mapped[Optional[str]] = mapped_column(String(255), comment="The hostname, pod name, or machine name where the error occurred.")
    node_group: Mapped[Optional[str]] = mapped_column(String(255), comment="The logical group of workers the source machine belongs to, if applicable.")

    context: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=True)
    stack_trace: Mapped[Optional[str]] = mapped_column(Text)
