# src/core/db_models/job_schema.py
"""
Defines the database schema for Master Jobs, Scan Templates, Policy Templates, and Jobs
using the SQLAlchemy ORM.
"""

import enum
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from sqlalchemy import (
    String, Integer, ForeignKey, DateTime, JSON, Boolean, func, Index, Enum as SQLAlchemyEnum, and_,
    text, BigInteger,LargeBinary
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, foreign, remote

from .base import Base

class JobType(str, enum.Enum):
    """Defines the overall goal or workflow of a job."""
    SCANNING = "SCANNING"
    POLICY = "POLICY"
    DB_PROFILE = "DB_PROFILE"
    BENCHMARK = "BENCHMARK"
    ENTITLEMENT = "ENTITLEMENT"
    VULNERABILITY = "VULNERABILITY"

class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    # NEW: Added for more descriptive final state
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES" 
    FAILED = "FAILED"

class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    COMPLETED = "COMPLETED"
    # NEW: Added for more descriptive final state
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES" 
    FAILED = "FAILED"


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
class ReportingScopeForLatest(str, enum.Enum):
    ALL = "ALL"
    CURRENT = "CURRENT"


# =================================================================
# NEW: MasterJob and MasterJobStateSummary Models
# =================================================================
class MasterJob(Base):
    __tablename__ = 'master_jobs'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_job_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    # NEW: Added JobStatus to MasterJob for high-level tracking
    status: Mapped[JobStatus] = mapped_column(SQLAlchemyEnum(JobStatus), nullable=False, default=JobStatus.QUEUED)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

# NEW: Complete schema for the state summary table
class MasterJobStateSummary(Base):
    __tablename__ = 'master_job_state_summary'
    __table_args__ = (
        Index('ix_summary_last_updated', 'last_updated'),
    )

    master_job_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    total_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    queued_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    running_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pausing_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    paused_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cancelling_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cancelled_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    completed_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_children: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    last_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    created_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

# =================================================================
# Template Models (Unchanged)
# =================================================================
# --- Define PolicyType Enum ---
class PolicyType(str, enum.Enum):
    REPORTING = "reporting"
    REMEDIATION_LIFECYCLE = "remediationlifecycle"
    POLICY_AND_ACTION = "policyandaction"
    ACTION = "action"
    POLICY_AND_TAG = "policyandtag"
    TAG = "tag"



# --- PolicyTemplate Update ---
class PolicyTemplate(Base):
    __tablename__ = 'policy_templates'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False) # of type :PolicyTemplateConfiguration

    # --- NEW FIELDS ---
    policy_type: Mapped[PolicyType] = mapped_column(
        SQLAlchemyEnum(PolicyType),
        nullable=False,
        comment="Defines the overall purpose of the policy."
    )
    
      
    # Storing the *master* job ID makes sense for grouping results of a full run.
    report_on_master_job_id: Mapped[Optional[str]] = mapped_column(
        String(255), # Master Job IDs are strings
        nullable=True,
        comment="If policy_type is 'reporting', specifies the master job ID whose data to query instead of 'latest'."
    )
    # --- END NEW FIELDS ---

    reporting_policy_template_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('policy_templates.id'),
        nullable=True,
        comment="Optional ID of a reporting policy to run after this policy completes."
    )
    reporting_policy: Mapped[Optional["PolicyTemplate"]] = relationship(remote_side=[id])

    jobs: Mapped[List["Job"]] = relationship(
        "Job",
        primaryjoin=lambda: and_(
            PolicyTemplate.id == foreign(Job.template_table_id),
            Job.template_type == JobType.POLICY # Use Enum here
        ),
        back_populates="policy_template",
        viewonly=True
    )


# --- ScanTemplate Update ---
class ScanTemplate(Base):
    __tablename__ = 'scan_templates'
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    template_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(1024))
    job_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False, default=JobType.SCANNING)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    classifier_template_id: Mapped[str] = mapped_column(String(255), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)

    # --- NEW FIELDS ---
    # Optional FK to a PolicyTemplate (which MUST have policytype="reporting")
    reporting_policy_template_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey('policy_templates.id'),
        nullable=True,
        comment="Optional ID of a reporting policy to run after this scan completes."
    )
    # Flag to control writing to the 'latest' S3 branch
    write_to_latest: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        comment="If True, results promote to 'latest' S3 path on job completion. Delta scans implicitly True."
    )
    _reporting_scope: Mapped[ReportingScopeForLatest] = mapped_column(SQLAlchemyEnum(ReportingScopeForLatest), nullable=False, default=ReportingScopeForLatest.ALL)
    # --- END NEW FIELDS ---

    # Optional relationship for easier access (optional)
    reporting_policy: Mapped[Optional["PolicyTemplate"]] = relationship()

    jobs: Mapped[List["Job"]] = relationship(
        "Job",
        primaryjoin=lambda: and_(
            ScanTemplate.id == foreign(Job.template_table_id),
            # Allow linking from various scanning job types
            Job.template_type.in_([JobType.SCANNING, JobType.DB_PROFILE, JobType.BENCHMARK, JobType.ENTITLEMENT, JobType.VULNERABILITY])
        ),
        back_populates="scan_template",
        viewonly=True
    )


# =================================================================
# Job Model (Unchanged)
# =================================================================
class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = (
        Index('ix_jobs_status', 'status'),
        Index(
            'ix_jobs_lease_for_recovery', 
            'node_group', 'status', 'orchestrator_lease_expiry',
            mssql_where=text("status = 'RUNNING'")
        ),
    )
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    master_job_id: Mapped[Optional[str]] = mapped_column(String(255), comment="The ID of the master job that created this child job.")
    master_pending_commands: Mapped[Optional[str]] = mapped_column(String(255), comment="Command issued by the master cli.")
    template_table_id: Mapped[int] = mapped_column(Integer, nullable=False)
    template_type: Mapped[JobType] = mapped_column(SQLAlchemyEnum(JobType), nullable=False)
    configuration: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="A self-contained copy of the filtered scan configuration.")
    node_group: Mapped[Optional[str]] = mapped_column(String(255), comment="The datacenter/region this job is assigned to.")
    orchestrator_id: Mapped[Optional[str]] = mapped_column(String(255), comment="The unique ID of the orchestrator instance that owns this job.")
    orchestrator_lease_expiry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="The timestamp when the current lease expires.")
    lease_warning_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="A counter for the failover grace period.")
    last_lease_warning_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), comment="Timestamp of the last lease expiry warning.")
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, comment="Version number for optimistic locking.")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    status: Mapped[JobStatus] = mapped_column(SQLAlchemyEnum(JobStatus), nullable=False, default=JobStatus.QUEUED)
    trigger_type: Mapped[str] = mapped_column(String(50), nullable=False)
    created_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    completed_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    scan_template: Mapped[Optional["ScanTemplate"]] = relationship(
        "ScanTemplate",
        primaryjoin=lambda: and_(
            remote(ScanTemplate.id) == foreign(Job.template_table_id),
            Job.template_type == 'SCANNING'
        ),
        back_populates="jobs",
        viewonly=True
    )
    
    policy_template: Mapped[Optional["PolicyTemplate"]] = relationship(
        "PolicyTemplate", 
        primaryjoin=lambda: and_(
            remote(PolicyTemplate.id) == foreign(Job.template_table_id),
            Job.template_type == 'POLICY'
        ),
        back_populates="jobs",
        viewonly=True
    )
    tasks: Mapped[List["Task"]] = relationship(cascade="all, delete-orphan")



# =================================================================
# Task Models
# =================================================================





# This class definition replaces the original Task class
class Task(Base):
    __tablename__ = 'tasks'
    __table_args__ = (
        Index('ix_tasks_status_job_id', 'status', 'job_id'),
        Index('ix_tasks_lease_expiry', 'lease_expiry'),
    )
    
    # FIXED: Changed the primary key from an Integer to a 32-byte hash (binary).
    # This will store the application-generated SHA-256 hash.
    id: Mapped[bytes] = mapped_column(LargeBinary(32), primary_key=True)
    
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id', ondelete="CASCADE"), nullable=False)
    datasource_id: Mapped[Optional[str]] = mapped_column(String(255))
    
    # FIXED: Changed the parent_task_id to also be a 32-byte hash to match the new primary key.
    parent_task_id: Mapped[Optional[bytes]] = mapped_column(LargeBinary(32))
    
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[TaskStatus] = mapped_column(SQLAlchemyEnum(TaskStatus), nullable=False, default=TaskStatus.PENDING, index=True)
    work_packet: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    worker_id: Mapped[Optional[str]] = mapped_column(String(255))
    lease_expiry: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    boundary_id: Mapped[Optional[bytes]] = mapped_column(LargeBinary(32), index=True, comment="A hash of the boundary (e.g., directory path) this output pertains to.")
    eligible_worker_type: Mapped[str] = mapped_column(String(100), nullable=False)
    node_group: Mapped[str] = mapped_column(String(100), nullable=False)    


class TaskOutputRecord(Base):
    __tablename__ = 'task_output_records'
    __table_args__ = (
        # Indexes are still important for query performance
        Index('ix_task_output_records_status_task_id', 'status', 'task_id'),
        Index('ix_task_output_records_job_id', 'job_id'), # Index for the new job_id
    )
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # NEW: Added a job_id for context and easier lookups.
    # It is indexed but does not have a foreign key constraint.
    job_id: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # FIXED: Changed task_id to a 32-byte hash to match the Task primary key.
    # It does not have a foreign key constraint.
    task_id: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False)
    
    status: Mapped[str] = mapped_column(String(50), nullable=False, default='PENDING_PROCESSING')
    output_type: Mapped[str] = mapped_column(String(100), nullable=False)
    output_payload: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False)
    created_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    boundary_id: Mapped[Optional[bytes]] = mapped_column(LargeBinary(32), index=True, comment="A hash of the boundary (e.g., directory path) this output pertains to.")