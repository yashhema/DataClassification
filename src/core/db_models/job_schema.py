# src/core/db_models/job_schema.py
"""
Defines the database schema for Scan Templates (Job Definitions) and Jobs
(Job Executions) using the SQLAlchemy ORM.
"""

import enum
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, func, Index, Enum as SQLAlchemyEnum
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base

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

class ScanTemplate(Base):
    __tablename__ = 'scan_templates'
    __table_args__ = {'extend_existing': True}
    __doc__ = "Stores the definition of a job."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="The unique string identifier for the scan template.")
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment="A human-readable name for the scan template.")
    description: Mapped[Optional[str]] = mapped_column(String(1024), comment="A brief description of the job's purpose.")
    job_type: Mapped[str] = mapped_column(String(50), nullable=False, comment="The type of job (e.g., 'scanning').")
    
    # NEWLY ADDED: Defines the priority for jobs created from this template.
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, comment="Default priority for jobs (lower number is higher priority).")
    
    classifier_template_id: Mapped[str] = mapped_column(String(255), nullable=False, comment="The ID of the classifier template to use.")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, comment="Whether this job definition is active.")
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="JSON object containing all detailed configurations like datasource_targets, schedule, etc.")
    jobs: Mapped[List["Job"]] = relationship(back_populates="scan_template")

class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'extend_existing': True}
    __doc__ = "Stores a record for a single execution (a run) of a ScanTemplate."
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="The unique identifier for this specific job run.")
    scan_template_id: Mapped[int] = mapped_column(ForeignKey('scan_templates.id'), nullable=False, comment="The ID of the ScanTemplate this job is an instance of.")
    
    # NEWLY ADDED: Stores the priority for this specific job execution.
    Priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100, index=True, comment="Job priority for this run (lower number is higher priority).")

    status: Mapped[JobStatus] = mapped_column(SQLAlchemyEnum(JobStatus), nullable=False, default=JobStatus.QUEUED, index=True)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False, comment="How the job was started (e.g., 'manual', 'scheduled').")
    created_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    completed_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime)
    scan_template: Mapped["ScanTemplate"] = relationship(back_populates="jobs")
    tasks: Mapped[List["Task"]] = relationship(back_populates="job")

class Task(Base):
    __tablename__ = 'Tasks'
    __table_args__ = (
        Index('IX_Tasks_Status_JobId', 'Status', 'JobID'),
        Index('IX_Tasks_LeaseExpiry', 'LeaseExpiry'),
        Index('IX_Tasks_WorkerID_Status', 'WorkerID', 'Status'),
        Index('IX_Tasks_DatasourceID_Status', 'DatasourceID', 'Status'),
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
    )
    __doc__ = "A lightweight communication table for job pipelining."
    
    ID: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    TaskID: Mapped[int] = mapped_column(Integer, nullable=False)
    Status: Mapped[str] = mapped_column(String(50), nullable=False, default='PENDING_PROCESSING')
    OutputType: Mapped[str] = mapped_column(String(100), nullable=False)
    OutputPayload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    CreatedTimestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))