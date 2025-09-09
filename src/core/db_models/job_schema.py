# src/core/db_models/job_schema.py
"""
Defines the database schema for Scan Templates (Job Definitions) and Jobs
(Job Executions) using the SQLAlchemy ORM.

UPDATED: This version introduces a formal JobType enum to distinguish
between different kinds of jobs, like standard scanning and policy execution.
"""

import enum
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, func, Index, Enum as SQLAlchemyEnum
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

# NEW: Formal enum for the different types of jobs the system can run.
class JobType(str, enum.Enum):
    """Defines the overall goal or workflow of a job."""
    SCANNING = "SCANNING"  # A standard discovery and classification job.
    POLICY = "POLICY"      # A job that executes a data governance policy (selects and acts).


class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
class PolicyTemplate(Base):
    __tablename__ = 'policy_templates'
    __doc__ = "Defines a data governance policy, including selection criteria and the action to be performed."

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="The unique string identifier for the policy.")
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment="A human-readable name for the policy.")
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    
    # This JSON blob will store the detailed, strongly-typed PolicyConfiguration model.
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

class ScanTemplate(Base):
    __tablename__ = 'scan_templates'
    __table_args__ = {'extend_existing': True}
    __doc__ = "Stores the definition of a job."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    
    # UPDATED: The job_type is now a strongly-typed enum.
    job_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False, default=JobType.SCANNING)
    
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    classifier_template_id: Mapped[str] = mapped_column(String(255), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    jobs: Mapped[List["Job"]] = relationship(back_populates="scan_template")

class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'extend_existing': True}
    __doc__ = "Stores a record for a single execution (a run) of any type of template."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    
    # UPDATED: The Job table now uses a polymorphic relationship to point to a template.
    template_table_id: Mapped[int] = mapped_column(Integer, nullable=False, comment="The primary key ID of the template (from either scan_templates or policy_templates).")
    template_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False, comment="The type of template this job is an instance of, indicating which table to join with.")
    
    Priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, index=True)
    status: Mapped[JobStatus] = mapped_column(SQLAlchemyEnum(JobStatus), nullable=False, default=JobStatus.QUEUED, index=True)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False)
    created_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    completed_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)
    tasks: Mapped[List["Task"]] = relationship()

class Task(Base):
    __tablename__ = 'Tasks'
    __table_args__ = (
        Index('IX_Tasks_Status_JobId', 'Status', 'JobID'),
        Index('IX_Tasks_LeaseExpiry', 'LeaseExpiry'),
        Index('IX_Tasks_WorkerID_Status', 'WorkerID', 'Status'),
        Index('IX_Tasks_DatasourceID_Status', 'DatasourceID', 'Status'),
        {'extend_existing': True}
    )
    __doc__ = "The central work queue. Contains every atomic unit of work for all jobs."

    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    JobID: Mapped[int] = mapped_column(ForeignKey('jobs.id'), nullable=False)
    DatasourceID: Mapped[Optional[str]] = mapped_column(String(255))
    ParentTaskID: Mapped[Optional[int]] = mapped_column(Integer)
    TaskType: Mapped[str] = mapped_column(String(100), nullable=False)
    Status: Mapped[TaskStatus] = mapped_column(SQLAlchemyEnum(TaskStatus), nullable=False, default=TaskStatus.PENDING, index=True)
    WorkPacket: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    WorkerID: Mapped[Optional[str]] = mapped_column(String(255))
    LeaseExpiry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    RetryCount: Mapped[int] = mapped_column(Integer, default=0)
    job: Mapped["Job"] = relationship(back_populates="tasks")

class TaskOutputRecord(Base):
    __tablename__ = 'TaskOutputRecords'
    __table_args__ = (
        Index('IX_TaskOutputRecords_Status_TaskID', 'Status', 'TaskID'),
        {'extend_existing': True}
    )
    __doc__ = "A lightweight communication table for job pipelining."
    
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TaskID: Mapped[int] = mapped_column(Integer, nullable=False)
    Status: Mapped[str] = mapped_column(String(50), nullable=False, default='PENDING_PROCESSING')
    OutputType: Mapped[str] = mapped_column(String(100), nullable=False)
    OutputPayload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    CreatedTimestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
