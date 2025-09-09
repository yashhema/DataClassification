# src/core/db_models/remediation_ledger_schema.py
"""
Defines the database schema for the Remediation Ledger, which stores the
actionable plans for Policy Jobs.
"""

import enum
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, JSON, DateTime, UniqueConstraint, Index, Enum as SQLAlchemyEnum
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

class LedgerStatus(str, enum.Enum):
    """Defines the state of a specific bin within a remediation plan."""
    PLANNED = "PLANNED"
    ACTION_IN_PROGRESS = "ACTION_IN_PROGRESS"
    ACTION_COMPLETED = "ACTION_COMPLETED"
    ACTION_FAILED = "ACTION_FAILED"
    RECONCILED = "RECONCILED" # Final state after metadata is updated

class RemediationLedger(Base):
    __tablename__ = 'RemediationLedger'
    __table_args__ = (
        UniqueConstraint('plan_id', 'bin_id', name='uq_plan_bin'),
        Index('ix_remediationledger_status', 'Status'),
        {'extend_existing': True}
    )
    __doc__ = """
    Stores the "bins" of work for the action phase of a Policy Job.
    Each row represents a self-contained batch of objects for a single worker to process.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    plan_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True, comment="The unique ID for the entire remediation plan, linking all bins together.")
    
    bin_id: Mapped[str] = mapped_column(String(255), nullable=False, comment="The unique ID for this specific bin of work within the plan.")

    Status: Mapped[LedgerStatus] = mapped_column(SQLAlchemyEnum(LedgerStatus), nullable=False, default=LedgerStatus.PLANNED)

    # The JSON columns are used to store the list of objects for the worker.
    ObjectIDs: Mapped[List[str]] = mapped_column(JSON, nullable=False, comment="The list of unique ObjectIDs for the worker to process in this bin.")
    
    ObjectPaths: Mapped[List[str]] = mapped_column(JSON, nullable=False, comment="The corresponding list of object paths needed by the connector.")

    # Optional fields for auditing and debugging.
    ResultDetails: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, comment="Stores a summary of the action's outcome, especially error details on failure.")
    
    WorkerID: Mapped[Optional[str]] = mapped_column(String(255), comment="The ID of the worker that last processed this bin.")
    
    LastUpdatedAt: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=datetime.now)