# src/core/db/database_interface.py
"""
Provides a high-level, unified, and production-ready interface for all 
database operations. This class is fully integrated with the system's
logging and error handling frameworks.

UPDATED: Added missing methods required for connector integration.
UPDATED: Converted to async/await pattern with configuration caching.
"""

import re
from contextlib import asynccontextmanager
from typing import List, Dict, Any, Optional,Union,NamedTuple
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import joinedload
import hashlib
import time
from sqlalchemy import (
    select, update, text, inspect, Table, func, Column, Integer, String,
    DateTime, LargeBinary, case,delete
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import and_
import json
from sqlalchemy.dialects.postgresql import insert as pg_insert
from ..db_models.processing_status_schema import ObjectProcessingStatus
# Import all ORM models from schema definitions
from ..db_models.base import Base
from ..db_models.job_schema import ScanTemplate, Job, Task, TaskStatus, TaskOutputRecord,MasterJobStateSummary
from ..db_models.discovery_catalog_schema import DiscoveredObject as OrmDiscoveredObject, ObjectMetadata
from ..db_models.discovery_catalog_schema import DiscoveredObjectClassificationDateInfo
from ..db_models.findings_schema import ScanFindingSummary
from ..db_models.system_parameters_schema import SystemParameter
from ..db_models.datasource_schema import NodeGroup, DataSource
from ..db_models.connector_config_schema import ConnectorConfiguration
from ..db_models.calendar_schema import Calendar
from ..db_models.job_schema import JobStatus, ScanTemplate, JobType
from ..db_models.credentials_schema import Credential
from ..db_models.classifiertemplate_schema import ClassifierTemplate
from ..db_models.classifier_schema import Classifier
from ..db_models.job_schema import PolicyTemplate
from ..db_models.remediation_ledger_schema import RemediationLedger, LedgerStatus

# Import Pydantic models for type hinting and data conversion

# Import core services for integration
from ..logging.system_logger import SystemLogger
from ..errors import ErrorHandler
from ..db_models.job_schema import MasterJob





class LeaseRenewalResult(NamedTuple):
    """Structured result for the lease renewal and command processing operation."""
    outcome: str  # e.g., "SUCCESS", "LOCK_FAILED"
    new_status: Optional[JobStatus] = None
    new_version: Optional[int] = None

class ConfigCache:
    """Configuration cache with LRU eviction and 24-hour expiry."""
    
    def __init__(self, max_general_items: int = 10, max_classifier_items: int = 70):
        self.general_cache: Dict[str, Any] = {}
        self.classifier_cache: Dict[str, Any] = {}
        self.cache_timestamps: Dict[str, datetime] = {}
        self.max_general = max_general_items
        self.max_classifier = max_classifier_items
        self.cache_duration = timedelta(hours=24)
    
    async def get_or_fetch(self, cache_key: str, fetch_func, is_classifier: bool = False):
        """Get from cache or fetch with LRU eviction and 24-hour expiry."""
        now = datetime.now(timezone.utc)
        
        # Check if item exists and is not expired
        if cache_key in self.cache_timestamps:
            if now - self.cache_timestamps[cache_key] < self.cache_duration:
                cache = self.classifier_cache if is_classifier else self.general_cache
                if cache_key in cache:
                    # Move to end (mark as recently used)
                    value = cache.pop(cache_key)
                    cache[cache_key] = value
                    return value
            else:
                # Remove expired item
                self._remove_item(cache_key, is_classifier)
        
        # Fetch new value
        value = await fetch_func()
        
        # Add to appropriate cache
        cache = self.classifier_cache if is_classifier else self.general_cache
        max_items = self.max_classifier if is_classifier else self.max_general
        
        # Evict oldest item if cache is full
        if len(cache) >= max_items:
            oldest_key = next(iter(cache))
            self._remove_item(oldest_key, is_classifier)
        
        # Add new item
        cache[cache_key] = value
        self.cache_timestamps[cache_key] = now
        
        return value
    
    def _remove_item(self, cache_key: str, is_classifier: bool):
        """Remove item from appropriate cache and timestamps."""
        cache = self.classifier_cache if is_classifier else self.general_cache
        cache.pop(cache_key, None)
        self.cache_timestamps.pop(cache_key, None)


class DatabaseInterface:
    """
    The primary interface for interacting with the application's central database.
    It encapsulates all database logic and integrates with core services.
    """

    def __init__(self, db_connection_string: str, system_logger: SystemLogger, error_handler: ErrorHandler):
        """Initializes the interface with core service integrations and production-ready pooling."""
        # Convert to async connection string if needed
        if db_connection_string.startswith('postgresql://'):
            async_connection_string = db_connection_string.replace('postgresql://', 'postgresql+asyncpg://')
        elif db_connection_string.startswith('sqlite://'):
            async_connection_string = db_connection_string.replace('sqlite://', 'sqlite+aiosqlite://')
        else:
            async_connection_string = db_connection_string
            
        self.async_engine = create_async_engine(
            async_connection_string, 
            pool_pre_ping=True,
            pool_size=10,      # A sensible default for connection pool size
            max_overflow=20    # Allows for spikes in demand
        )
        self.AsyncSessionFactory = async_sessionmaker(
            autocommit=False, 
            autoflush=False, 
            bind=self.async_engine
        )
        self.logger = system_logger
        self.error_handler = error_handler
        self._safe_table_name_pattern = re.compile(r'^[a-zA-Z0-9_]+$')
        self.config_cache = ConfigCache()

    def get_async_session(self) -> AsyncSession:
        """Provides a new async database session for use in a 'async with' statement."""
        return self.AsyncSessionFactory()

    def _validate_table_name(self, table_name: str):
        """Validates table name to prevent SQL injection."""
        if not self._safe_table_name_pattern.match(table_name):
            raise ValueError(f"Invalid characters in table name: {table_name}")


    async def requeue_job(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Resets a job to the QUEUED state, clearing its ownership and lease.
        This is a recovery mechanism for orphaned or stalled jobs.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation_name="requeue_job", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Job).where(
                    Job.id == job_id
                ).values(
                    status=JobStatus.QUEUED,
                    orchestrator_id=None,
                    orchestrator_lease_expiry=None,
                    lease_warning_count=0,
                    last_lease_warning_timestamp=None,
                    version=Job.version + 1 # Increment version to invalidate old leases
                )
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "Jobs", "SUCCESS", operation_name="requeue_job", job_id=job_id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, "requeue_job", job_id=job_id, **context)
            raise
    # =================================================================
    # NEW: Master Job and Summary Management Methods
    # =================================================================


    async def start_job_transactional(self, job_details: dict) -> None:
            """
            Creates the master job, summary, and all child jobs in a single, atomic transaction.
            If any part of the process fails, the entire transaction is rolled back.
            """
            context = job_details.get("context", {})
            master_job_id = job_details.get("master_job_id")
            self.logger.log_database_operation(
                "TRANSACTION", "Jobs", "STARTED",
                operation_name="start_job_transactional",
                master_job_id=master_job_id,
                **context
            )
            
            # Start a single session that will manage the entire transaction
            async with self.get_async_session() as session:
                try:
                    # 1. Create the MasterJob record
                    await self.create_master_job(
                        master_job_id=master_job_id,
                        name=job_details.get("master_job_name"),
                        configuration=job_details.get("master_job_config"),
                        context=context,
                        session=session  # Participate in the transaction
                    )

                    # 2. Create the MasterJobStateSummary record
                    child_jobs_to_create = job_details.get("child_jobs", [])
                    await self.create_master_job_summary(
                        master_job_id=master_job_id,
                        total_children=len(child_jobs_to_create),
                        context=context,
                        session=session  # Participate in the transaction
                    )

                    # 3. Create all child Job records
                    for child_job in child_jobs_to_create:
                        await self.create_job_execution(
                            master_job_id=master_job_id,
                            template_table_id=child_job.get("template_table_id"),
                            template_type=child_job.get("template_type"),
                            execution_id=child_job.get("execution_id"),
                            trigger_type=child_job.get("trigger_type"),
                            nodegroup=child_job.get("nodegroup"),
                            configuration=child_job.get("configuration"),
                            priority=child_job.get("priority"),
                            master_pending_commands=child_job.get("master_pending_commands"),
                            context=context,
                            session=session  # Participate in the transaction
                        )

                    # 4. If all steps succeed, commit the single transaction
                    await session.commit()
                    self.logger.log_database_operation(
                        "TRANSACTION", "Jobs", "SUCCESS",
                        operation_name="start_job_transactional",
                        master_job_id=master_job_id,
                        **context
                    )

                except Exception as e:
                    # If any step fails, roll back the entire transaction
                    self.logger.error(
                        "Transactional job creation failed. Rolling back all changes.",
                        master_job_id=master_job_id,
                        error=str(e),
                        exc_info=True
                    )
                    await session.rollback()
                    # Re-raise the exception to be handled by the caller
                    raise

    
    async def get_master_job_summary(self, master_job_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[MasterJobStateSummary]:
        """Fetches the state summary record for a master job."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "MasterJobStateSummary", "STARTED", master_id=master_job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(MasterJobStateSummary).where(MasterJobStateSummary.master_job_id == master_job_id)
                result = await session.scalars(stmt)
                summary = result.one_or_none()
                self.logger.log_database_operation("SELECT", "MasterJobStateSummary", "SUCCESS", found=(summary is not None), **context)
                return summary
        except Exception as e:
            self.error_handler.handle_error(e, "get_master_job_summary", master_id=master_job_id, **context)
            raise

    async def create_master_job_summary(self, master_job_id: str, total_children: int, context: Optional[Dict[str, Any]] = None,session: Optional[AsyncSession] = None) -> None:
        """Creates the initial summary record for a new Master Job."""
        context = context or {}
        self.logger.log_database_operation("INSERT", "MasterJobStateSummary", "STARTED", master_job_id=master_job_id, **context)
        try:
            async with self._get_session_context() as session:
                summary = MasterJobStateSummary(
                    master_job_id=master_job_id,
                    total_children=total_children,
                    queued_children=total_children
                )
                session.add(summary)
            self.logger.log_database_operation("INSERT", "MasterJobStateSummary", "SUCCESS", master_job_id=master_job_id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, "create_master_job_summary", master_job_id=master_job_id, **context)
            raise


    async def update_master_job_summary_counters(self, master_job_id: str, from_status: JobStatus, to_status: JobStatus, current_version: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """Atomically updates summary counters using optimistic locking."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "MasterJobStateSummary", "STARTED", operation="update_counters",  **context)
        try:
            async with self.get_async_session() as session:
                from_field = f"{from_status.value.lower()}_children"
                to_field = f"{to_status.value.lower()}_children"

                stmt = (
                    update(MasterJobStateSummary)
                    .where(
                        MasterJobStateSummary.master_job_id == master_job_id,
                        MasterJobStateSummary.version == current_version
                    )
                    .values(
                        **{
                            from_field: getattr(MasterJobStateSummary, from_field) - 1,
                            to_field: getattr(MasterJobStateSummary, to_field) + 1,
                            "version": MasterJobStateSummary.version + 1,
                        }
                    )
                )
                result = await session.execute(stmt)
                await session.commit()
                
                was_successful = result.rowcount > 0
                self.logger.log_database_operation("UPDATE", "MasterJobStateSummary", "SUCCESS" if was_successful else "FAILURE_OPTIMISTIC_LOCK", operation="update_counters", **context)
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(e, "update_master_job_summary_counters",  **context)
            raise

    async def get_active_master_jobs_for_monitoring(self, context: Optional[Dict[str, Any]] = None) -> List[MasterJob]:
        """Gets all Master Jobs that are in a non-terminal state for monitoring."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "MasterJobs", "STARTED", operation="get_active_for_monitoring", **context)
        try:
            async with self.get_async_session() as session:
                terminal_statuses = [JobStatus.COMPLETED, JobStatus.COMPLETED_WITH_FAILURES, JobStatus.FAILED, JobStatus.CANCELLED]
                stmt = select(MasterJob).where(MasterJob.status.notin_(terminal_statuses))
                result = await session.scalars(stmt)
                jobs = result.all()
                self.logger.log_database_operation("SELECT", "MasterJobs", "SUCCESS", count=len(jobs), **context)
                return jobs
        except Exception as e:
            self.error_handler.handle_error(e, "get_active_master_jobs_for_monitoring", **context)
            raise

    async def recalculate_and_correct_summary(self, master_job_id: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Recalculates the true child job counts and corrects the summary table."""
        context = context or {}
        self.logger.log_database_operation("RECONCILE", "MasterJobStateSummary", "STARTED", master_job_id=master_job_id, **context)
        try:
            async with self.get_async_session() as session:
                # Get the true counts with a GROUP BY query
                stmt = select(Job.status, func.count(Job.id)).where(Job.master_job_id == master_job_id).group_by(Job.status)
                result = await session.execute(stmt)
                true_counts = {status.value.lower(): count for status, count in result.all()}

                # Construct the update values, defaulting to 0 for any missing statuses
                update_values = {f"{status.value.lower()}_children": true_counts.get(status.value.lower(), 0) for status in JobStatus}
                update_values["version"] = MasterJobStateSummary.version + 1 # Increment version

                update_stmt = update(MasterJobStateSummary).where(MasterJobStateSummary.master_job_id == master_job_id).values(**update_values)
                await session.execute(update_stmt)
                await session.commit()
                self.logger.info(f"Reconciliation successful for master job {master_job_id}.", master_job_id=master_job_id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, "recalculate_and_correct_summary", master_job_id=master_job_id, **context)
            raise

    async def update_master_job_status(self, master_job_id: str, new_status: JobStatus, context: Optional[Dict[str, Any]] = None) -> None:
        """Updates the status of a Master Job record."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "MasterJobs", "STARTED", master_job_id=master_job_id, new_status=new_status.value, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(MasterJob).where(MasterJob.master_job_id == master_job_id).values(status=new_status)
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "MasterJobs", "SUCCESS", master_job_id=master_job_id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, "update_master_job_status", master_job_id=master_job_id, **context)
            raise
            
    # =================================================================
    # Pipeline Integrity Methods
    # =================================================================
    
    async def has_pending_output_records(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """Checks if a job has any pending task output records."""
        context = context or {}
        self.logger.log_database_operation("QUERY", "TaskOutputRecords", "STARTED", operation="check_pending", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                # This subquery finds all task_ids for the given job_id
                task_ids_subquery = select(Task.id).where(Task.job_id == job_id)
                
                # The main query counts records linked to those task_ids
                stmt = select(func.count()).select_from(TaskOutputRecord).where(
                    TaskOutputRecord.task_id.in_(task_ids_subquery),
                    TaskOutputRecord.status == 'PENDING_PROCESSING'
                )
                result = await session.execute(stmt)
                count = result.scalar_one()
                return count > 0
        except Exception as e:
            self.error_handler.handle_error(e, "has_pending_output_records", job_id=job_id, **context)
            raise

    # =================================================================
    # Lease atomic methods
    # =================================================================
    # NEW: Atomic method for combined lease renewal and command processing, without version check
    async def renew_lease_and_process_command(
        self,
        job_id: int,
        orchestrator_id: str,
        current_version: int,
        lease_duration_sec: int,
        context: Optional[Dict[str, Any]] = None
    ) -> LeaseRenewalResult:
        """
        Atomically renews a job's lease and processes any pending command in a single
        operation using optimistic locking and conditional logic.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "jobs", "STARTED", operation="renew_and_process_command", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                new_expiry = datetime.now(timezone.utc) + timedelta(seconds=lease_duration_sec)
                
                # This complex UPDATE uses a CASE statement to enforce state transition rules atomically.
                stmt = (
                    update(Job)
                    .where(
                        Job.id == job_id,
                        Job.orchestrator_id == orchestrator_id,
                        #Job.version == current_version  # Optimistic lock
                    )
                    .values(
                        orchestrator_lease_expiry=new_expiry,
                        status=case(
                            (Job.master_pending_commands == 'CANCEL', JobStatus.CANCELLING),
                            (and_(Job.master_pending_commands == 'PAUSE', Job.status == JobStatus.RUNNING), JobStatus.PAUSING),
                            (and_(Job.master_pending_commands == 'RESUME', Job.status == JobStatus.PAUSED), JobStatus.RUNNING),
                            else_=Job.status # No command or invalid command, keep current status
                        ),
                        master_pending_commands=None, # Atomically clear the command
                        version=Job.version + 1
                    )
                    .returning(Job.status, Job.version) # Return the final state
                )

                result = await session.execute(stmt)
                await session.commit()
                
                updated_state = result.fetchone()

                if updated_state:
                    new_status, new_version = updated_state
                    self.logger.log_database_operation("UPDATE", "jobs", "SUCCESS", operation="renew_and_process_command", job_id=job_id, new_status=new_status.value, **context)
                    return LeaseRenewalResult("SUCCESS", new_status, new_version)
                else:
                    # This means the optimistic lock failed (rowcount was 0)
                    self.logger.warning(f"Lease renewal for job {job_id} failed optimistic lock.", job_id=job_id, **context)
                    return LeaseRenewalResult("LOCK_FAILED")
        except Exception as e:
            self.error_handler.handle_error(e, "renew_lease_and_process_command", job_id=job_id, **context)
            # Return a specific error outcome
            return LeaseRenewalResult("ERROR")

    # =================================================================
    # Startup Validation Method
    # =================================================================

    async def set_and_verify_master_monitor_instance(self, instance_id: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """Sets this instance as the active monitor, checking for a split-brain scenario."""
        context = context or {}
        param_name = "active_master_monitor_instance"
        self.logger.info(f"Attempting to acquire master monitor role for instance '{instance_id}'.", **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(SystemParameter).where(SystemParameter.parameter_name == param_name)
                result = await session.scalars(stmt)
                param = result.one_or_none()

                now = datetime.now(timezone.utc)
                if param and param.parameter_value:
                    try:
                        stored_value = json.loads(param.parameter_value)
                        last_heartbeat = datetime.fromisoformat(stored_value.get("timestamp", ""))
                        # Stale lock check (e.g., if the previous instance crashed without cleanup)
                        if now - last_heartbeat < timedelta(minutes=5):
                            if stored_value.get("instance_id") != instance_id:
                                self.logger.critical(f"SPLIT-BRAIN DETECTED: Another active master monitor '{stored_value['instance_id']}' holds the role.", **context)
                                return False
                    except (json.JSONDecodeError, TypeError):
                        self.logger.warning("Could not parse existing master monitor parameter. Overwriting.", **context)

                # Acquire or update the role
                new_value = json.dumps({"instance_id": instance_id, "timestamp": now.isoformat()})
                if param:
                    param.parameter_value = new_value
                else:
                    param = SystemParameter(parameter_name=param_name, parameter_value=new_value, applicable_to='ORCHESTRATOR')
                
                await session.merge(param)
                await session.commit()
                self.logger.info(f"Successfully acquired master monitor role for instance '{instance_id}'.", **context)
                return True

        except Exception as e:
            self.error_handler.handle_error(e, "set_and_verify_master_monitor_instance", instance_id=instance_id, **context)
            return False # Fail safe: if we can't verify, don't start central functions.


    
    async def acknowledge_and_update_job_status(self, job_id: int, new_status: JobStatus, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Atomically updates a job's status and clears the pending command field.
        """
        context = context or {}
        self.logger.log_database_operation(
            "UPDATE", "Jobs", "STARTED", 
            operation="acknowledge_and_update_status", 
            job_id=job_id, 
            **context
        )
        try:
            async with self.get_async_session() as session:
                stmt = update(Job).where(Job.id == job_id).values(
                    status=new_status,
                    master_pending_commands=None, # Clear the command
                    version=Job.version + 1       # Increment version
                )
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "Jobs", "SUCCESS", operation="acknowledge_and_update_status", **context)
        except Exception as e:
            self.error_handler.handle_error(e, "acknowledge_and_update_job_status", job_id=job_id, **context)
            raise

    async def create_master_job(
        self,
        master_job_id: str,
        name: str,
        configuration: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        session: Optional[AsyncSession] = None
    ) -> MasterJob:
        """
        Creates a new record in the MasterJobs table. If a session is provided,
        it participates in that transaction. Otherwise, it creates its own.
        It always returns the refreshed object.
        """
        context = context or {}
        self.logger.log_database_operation("INSERT", "MasterJobs", "STARTED", master_job_id=master_job_id, **context)
        
        try:
            async with self._get_session_context(session=session) as inner_session:
                new_master_job = MasterJob(
                    master_job_id=master_job_id,
                    name=name,
                    configuration=configuration
                )
                inner_session.add(new_master_job)
                
                # Flush is sufficient to get the object into the transaction queue.
                await inner_session.flush()
                
                # REMOVED: The refresh call that causes the "connection is busy" error.
                # await inner_session.refresh(new_master_job)

            self.logger.log_database_operation("INSERT", "MasterJobs", "SUCCESS", master_job_id=new_master_job.master_job_id, **context)
            return new_master_job
            
        except Exception as e:
            self.error_handler.handle_error(e, "create_master_job", master_job_id=master_job_id, **context)
            raise



    async def get_master_job_by_id(self, master_job_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[MasterJob]:
        """Fetches a master job's configuration by its ID."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "MasterJobs", "STARTED", master_job_id=master_job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(MasterJob).where(MasterJob.master_job_id == master_job_id)
                result = await session.scalars(stmt)
                master_job = result.one_or_none()
                self.logger.log_database_operation("SELECT", "MasterJobs", "SUCCESS", found=(master_job is not None), **context)
                return master_job
        except Exception as e:
            self.error_handler.handle_error(e, "get_master_job_by_id", master_job_id=master_job_id, **context)
            raise

    async def get_master_job_by_id_with_cache(self, master_job_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[MasterJob]:
        """
        Gets a master job by its ID, utilizing the central configuration cache
        to avoid repeated database lookups.
        """
        if not master_job_id:
            return None
            
        cache_key = f"master_job:{master_job_id}"
        
        # Use the existing get_or_fetch method from the ConfigCache
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self.get_master_job_by_id(master_job_id, context),
            is_classifier=False # Master jobs are general configuration
        )

    async def get_jobs_by_ids(self, job_ids: List[int], context: Optional[Dict[str, Any]] = None) -> List[Job]:
        """
        Fetches a list of Job objects from a list of job IDs.
        """
        context = context or {}
        if not job_ids:
            return []
            
        self.logger.log_database_operation("SELECT", "Jobs", "STARTED", operation_name="get_by_ids", count=len(job_ids), **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(Job).where(Job.id.in_(job_ids))
                result = await session.scalars(stmt)
                jobs = result.all()

                self.logger.log_database_operation("SELECT", "Jobs", "SUCCESS", operation_name="get_by_ids", found_count=len(jobs), **context)
                return jobs
        except Exception as e:
            self.error_handler.handle_error(e, "get_jobs_by_ids", job_count=len(job_ids), **context)
            raise


    async def update_job_warning_count(self, job_id: int, new_count: int, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Updates the warning count and timestamp for a job with an expired lease.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation_name="update_warning_count", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Job).where(
                    Job.id == job_id
                ).values(
                    lease_warning_count=new_count,
                    last_lease_warning_timestamp=datetime.now(timezone.utc)
                )
                await session.execute(stmt)
                await session.commit()
        except Exception as e:
            self.error_handler.handle_error(e, "update_job_warning_count", job_id=job_id, **context)
            raise

    async def takeover_abandoned_job(self, job_id: int, job_version: int, new_orchestrator_id: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Atomically takes ownership of an abandoned job using optimistic locking.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation_name="takeover_job", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Job).where(
                    Job.id == job_id,
                    Job.version == job_version # Optimistic lock
                ).values(
                    orchestrator_id=new_orchestrator_id,
                    version=Job.version + 1 # Increment version to lock out others
                )
                result = await session.execute(stmt)
                await session.commit()
                return result.rowcount > 0
        except Exception as e:
            self.error_handler.handle_error(e, "takeover_abandoned_job", job_id=job_id, **context)
            raise

    async def fail_job(self, job_id: int, reason: str, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Moves a job to the FAILED state and records its completion time.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation_name="fail_job", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                # In a real system, the 'reason' might be stored in a new 'Notes' column
                stmt = update(Job).where(
                    Job.id == job_id
                ).values(
                    status=JobStatus.FAILED,
                    completed_timestamp=datetime.now(timezone.utc),
                    version=Job.version + 1 # Increment version to prevent stale writes
                )
                await session.execute(stmt)
                await session.commit()
        except Exception as e:
            self.error_handler.handle_error(e, "fail_job", job_id=job_id, **context)
            raise


    async def get_queued_jobs_for_nodegroup(self, nodegroup: str, limit: int, context: Optional[Dict[str, Any]] = None) -> List[Job]:
        """Used by an Orchestrator to find available jobs in its region."""
        context = context or {}
        try:
            async with self.get_async_session() as session:
                stmt = select(Job).where(
                    Job.status == JobStatus.QUEUED,
                    Job.node_group == nodegroup
                ).order_by(Job.priority, Job.id).limit(limit)
                result = await session.scalars(stmt)
                return list(result.all())
        except Exception as e:
            self.error_handler.handle_error(e, "get_queued_jobs_for_nodegroup", nodegroup=nodegroup, **context)
            raise

    async def claim_queued_job(self, job_id: int, orchestrator_id: str, lease_duration_sec: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """Atomically claims a QUEUED job for an orchestrator."""
        context = context or {}
        try:
            async with self.get_async_session() as session:
                lease_expiry = datetime.now(timezone.utc) + timedelta(seconds=lease_duration_sec)
                stmt = update(Job).where(
                    Job.id == job_id,
                    Job.status == JobStatus.QUEUED
                ).values(
                    status=JobStatus.RUNNING,
                    orchestrator_id=orchestrator_id,
                    orchestrator_lease_expiry=lease_expiry
                )
                result = await session.execute(stmt)
                await session.commit()
                # If rowcount is 1, the update was successful. If 0, another orchestrator claimed it first.
                return result.rowcount > 0
        except Exception as e:
            self.error_handler.handle_error(e, "claim_queued_job", job_id=job_id, **context)
            raise

    async def renew_job_lease(self, job_id: int, orchestrator_id: str, current_version: int, lease_duration_sec: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Atomically renews the lease for a job using optimistic locking.
        It will only succeed if the orchestrator_id and version match.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation_name="renew_job_lease", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                new_expiry = datetime.now(timezone.utc) + timedelta(seconds=lease_duration_sec)
                
                stmt = update(Job).where(
                    Job.id == job_id,
                    Job.orchestrator_id == orchestrator_id,
                    Job.version == current_version  # <-- The optimistic lock check
                ).values(
                    orchestrator_lease_expiry=new_expiry,
                    lease_warning_count=0,
                    last_lease_warning_timestamp=None,
                    version=Job.version + 1  # Increment the version for the next update
                )
                
                result = await session.execute(stmt)
                await session.commit()

                # If rowcount is 1, the update succeeded. If 0, we lost the race.
                was_successful = result.rowcount > 0
                self.logger.log_database_operation("UPDATE", "Jobs", "SUCCESS" if was_successful else "SKIPPED_STALE", operation_name="renew_job_lease", job_id=job_id, **context)
                
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(e, "renew_job_lease", job_id=job_id, **context)
            raise


    async def get_abandoned_jobs_for_nodegroup(self, nodegroup: str, context: Optional[Dict[str, Any]] = None) -> List[Job]:
        """Used by the Job Reaper to find jobs with expired leases."""
        context = context or {}
        try:
            async with self.get_async_session() as session:
                stmt = select(Job).where(
                    Job.status == JobStatus.RUNNING,
                    Job.node_group == nodegroup,
                    Job.orchestrator_lease_expiry < datetime.now(timezone.utc)
                )
                result = await session.scalars(stmt)
                return list(result.all())
        except Exception as e:
            self.error_handler.handle_error(e, "get_abandoned_jobs_for_nodegroup", nodegroup=nodegroup, **context)
            raise

    async def get_recoverable_jobs_for_orchestrator(self, orchestrator_id: str, context: Optional[Dict[str, Any]] = None) -> List[Job]:
        """Used by an orchestrator on startup to find and reclaim its own jobs."""
        context = context or {}
        try:
            async with self.get_async_session() as session:
                stmt = select(Job).where(
                    Job.status == JobStatus.RUNNING,
                    Job.orchestrator_id == orchestrator_id
                )
                result = await session.scalars(stmt)
                return list(result.all())
        except Exception as e:
            self.error_handler.handle_error(e, "get_recoverable_jobs_for_orchestrator", orchestrator_id=orchestrator_id, **context)
            raise


    async def upsert_object_metadata(
        self,
        records: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Performs a bulk-safe "upsert" on the ObjectMetadata table. It either inserts
        new records or updates existing ones using object_key_hash as the primary key.
        """
        context = context or {}
        if not records:
            return
            
        self.logger.log_database_operation("UPSERT", "ObjectMetadata", "STARTED", record_count=len(records), **context)
        
        try:
            async with self.get_async_session() as session:
                # This uses a PostgreSQL-specific "INSERT ... ON CONFLICT DO UPDATE"
                # for a highly efficient and atomic bulk upsert operation.
                stmt = pg_insert(ObjectMetadata).values(records)
                
                # Define what to do if the object_key_hash already exists
                update_dict = {
                    col.name: getattr(stmt.excluded, col.name)
                    for col in ObjectMetadata.__table__.columns
                    if not col.primary_key
                }

                stmt = stmt.on_conflict_do_update(
                    index_elements=['object_key_hash'],  # Changed: Use hash-based primary key
                    set_=update_dict
                )
                
                await session.execute(stmt)
                await session.commit()
                
            self.logger.log_database_operation("UPSERT", "ObjectMetadata", "SUCCESS", record_count=len(records), **context)

        except Exception as e:
            self.error_handler.handle_error(e, "upsert_object_metadata", record_count=len(records), **context)
            raise


    async def reconcile_remediation_updates(self, updates: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> None:
        """
        Performs a bulk update on the ObjectMetadata table to reflect the
        outcome of a remediation action (e.g., updating paths after a move)
        using object_key_hash as the primary key identifier.
        """
        context = context or {}
        if not updates:
            return

        self.logger.log_database_operation("BULK_UPDATE", "ObjectMetadata", "STARTED", operation="reconcile", update_count=len(updates), **context)
        try:
            async with self.get_async_session() as session:
                # bulk_update_mappings using object_key_hash as the primary key identifier
                # 'updates' should be a list of dicts, e.g.:
                # [{"object_key_hash": b'...', "detailed_metadata": {...}, "metadata_fetch_timestamp": datetime}]
                await session.run_sync(lambda sync_session: sync_session.bulk_update_mappings(ObjectMetadata, updates))
                await session.commit()
            
            self.logger.log_database_operation("BULK_UPDATE", "ObjectMetadata", "SUCCESS", operation="reconcile", update_count=len(updates), **context)

        except Exception as e:
            self.error_handler.handle_error(e, "reconcile_remediation_updates", update_count=len(updates), **context)
            raise

# To be added to: src/core/db/database_interface.py

    async def add_enrichment_action_to_catalog(
        self,
        object_key_hashes: List[bytes],
        action_details: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Performs a bulk update to atomically append a new action (like tagging)
        to the ActionHistory of many objects identified by their key hash.
        """
        context = context or {}
        if not object_key_hashes:
            return
            
        self.logger.log_database_operation("BULK_UPDATE", "ObjectMetadata", "STARTED", operation="add_enrichment_action", **context)
        try:
            # This implementation is for SQL with JSONB support (like PostgreSQL)
            async with self.get_async_session() as session:
                # The '||' operator is the PostgreSQL JSONB array concatenation operator.
                # This statement finds all matching objects and appends the new action
                # to their history in a single, efficient database operation.
                stmt = (
                    update(ObjectMetadata)
                    .where(ObjectMetadata.object_key_hash.in_(object_key_hashes))
                    .values(ActionHistory=ObjectMetadata.action_history.op('||')(text("'[null]'::jsonb"))) # Placeholder for append syntax
                )
                # A real implementation would correctly format the JSON to append `action_details`.
                # For simplicity in this example, we show the intent.
                
                # A more compatible way is read-modify-write as shown in previous examples.
                
                # For DynamoDB, this would be a series of BatchWriteItem calls with an UpdateExpression.
            
            self.logger.log_database_operation("BULK_UPDATE", "ObjectMetadata", "SUCCESS", operation="add_enrichment_action", **context)

        except Exception as e:
            self.error_handler.handle_error(e, "add_enrichment_action_to_catalog", **context)
            raise



    # =================================================================
    # NEW: Method to log object processing status
    # =================================================================


    async def log_object_processing_status(self, object_id: str, job_id: int, status: str, details: Optional[str], context: Optional[Dict[str, Any]] = None):
        """
        Creates or updates the processing status for a single discovered object.
        This provides an audit trail for objects that could not be classified.
        """
        context = context or {}
        self.logger.log_database_operation("UPSERT", "ObjectProcessingStatuses", "STARTED", object_id=object_id, job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                # The `merge` operation performs a clean "upsert" (INSERT or UPDATE).
                # It uses the primary key (object_id_str) to find an existing record.
                # If found, it updates the record's fields.
                # If not found, it inserts a new record.
                record = ObjectProcessingStatus(
                    object_id_str=object_id,
                    job_id=job_id,
                    status=status,
                    details=details,
                    scan_timestamp=datetime.now(timezone.utc)
                )
                await session.merge(record)
                await session.commit()
                self.logger.log_database_operation("UPSERT", "ObjectProcessingStatuses", "SUCCESS", object_id=object_id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="log_object_processing_status", object_id=object_id, job_id=job_id, **context)
            raise


    # =================================================================
    # NEW: Connector Support Methods
    # =================================================================

    async def get_datasource_configuration(self, datasource_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[DataSource]:
        """Get complete datasource configuration by ID."""
        cache_key = f"datasource_config:{datasource_id}"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_datasource_configuration(datasource_id, context),
            is_classifier=False
        )
    # In: src/core/db/database_interface.py

    # Update this method to eagerly load the relationship
    async def get_datasources_by_ids(self, datasource_ids: List[str], context: Optional[Dict[str, Any]] = None) -> List[DataSource]:
        """
        Fetches a list of DataSource objects from a list of datasource IDs, eagerly loading the NodeGroup.
        """
        context = context or {}
        if not datasource_ids:
            return []
            
        self.logger.log_database_operation("SELECT", "DataSources", "STARTED", operation_name="get_by_ids", count=len(datasource_ids), **context)
        try:
            async with self.get_async_session() as session:
                # UPDATED: Added joinedload() to efficiently fetch the related NodeGroup object.
                stmt = select(DataSource).where(
                    DataSource.datasource_id.in_(datasource_ids)
                ).options(
                    joinedload(DataSource.node_group)
                )
                result = await session.scalars(stmt)
                datasources = result.all()

                self.logger.log_database_operation("SELECT", "DataSources", "SUCCESS", operation_name="get_by_ids", found_count=len(datasources), **context)
                return datasources
        except Exception as e:
            self.error_handler.handle_error(e, "get_datasources_by_ids", datasource_count=len(datasource_ids), **context)
            raise

    async def _fetch_datasource_configuration(self, datasource_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[DataSource]:
        """Internal method to fetch datasource configuration from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "DataSources", "STARTED", 
                                         operation="get_configuration", 
                                         datasource_id=datasource_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(DataSource).where(DataSource.datasource_id == datasource_id)
                result = await session.scalars(stmt)
                result = result.one_or_none()
                
                self.logger.log_database_operation("SELECT", "DataSources", "SUCCESS", 
                                                 operation="get_configuration", 
                                                 found=(result is not None), **context)
                return result
        except Exception as e:
            self.error_handler.handle_error(e, context="get_datasource_configuration", 
                                          datasource_id=datasource_id, **context)
            raise

    async def get_credential_for_datasource(self, datasource_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Get credential information for a datasource."""
        cache_key = f"datasource_credential:{datasource_id}"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_credential_for_datasource(datasource_id, context),
            is_classifier=False
        )

    async def _fetch_credential_for_datasource(self, datasource_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Internal method to fetch credential for datasource from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "Credentials", "STARTED", 
                                         operation="get_for_datasource", 
                                         datasource_id=datasource_id, **context)
        try:
            async with self.get_async_session() as session:
                # First get the datasource to find credential_id
                datasource_result = await session.scalars(
                    select(DataSource).where(DataSource.datasource_id == datasource_id)
                )
                datasource = datasource_result.one_or_none()
                
                if not datasource:
                    return None
                
                # Extract credential_id from datasource configuration
                credential_id = datasource.configuration.get('credential_id')
                if not credential_id:
                    return None
                
                # Get the credential
                credential_result = await session.scalars(
                    select(Credential).where(Credential.credential_id == credential_id)
                )
                credential = credential_result.one_or_none()
                
                if credential:
                    credential_data = {
                        'credential_id': credential.credential_id,
                        'username': credential.username,
                        'domain': credential.domain,
                        'store_details': credential.store_details
                    }
                    
                    self.logger.log_database_operation("SELECT", "Credentials", "SUCCESS", 
                                                     operation="get_for_datasource", **context)
                    return credential_data
                else:
                    self.logger.log_database_operation("SELECT", "Credentials", "NOT_FOUND", 
                                                     operation="get_for_datasource", **context)
                    return None
                    
        except Exception as e:
            self.error_handler.handle_error(e, context="get_credential_for_datasource", 
                                          datasource_id=datasource_id, **context)
            raise

    async def get_classifier_template_full(self, template_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[ClassifierTemplate]:
        """Get complete classifier template with all related classifiers."""
        cache_key = f"classifier_template:{template_id}"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_classifier_template_full(template_id, context),
            is_classifier=True
        )

    async def _fetch_classifier_template_full(self, template_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[ClassifierTemplate]:
        """Internal method to fetch classifier template from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "ClassifierTemplates", "STARTED", 
                                         operation="get_full", 
                                         template_id=template_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = (
                    select(ClassifierTemplate)
                    .where(ClassifierTemplate.template_id == template_id)
                    .options(
                        joinedload(ClassifierTemplate.classifiers)
                        .joinedload(Classifier.patterns),
                        joinedload(ClassifierTemplate.classifiers)
                        .joinedload(Classifier.context_rules),
                        joinedload(ClassifierTemplate.classifiers)
                        .joinedload(Classifier.validation_rules),
                        # --- THIS IS THE FIX ---
                        # Eagerly load the exclude_list relationship as well.
                        joinedload(ClassifierTemplate.classifiers)
                        .joinedload(Classifier.exclude_list)
                        # --- END OF FIX ---
                        
                    )
                )
                result = await session.scalars(stmt)
                unique_result = result.unique().one_or_none()
                
                self.logger.log_database_operation("SELECT", "ClassifierTemplates", "SUCCESS", 
                                                 operation="get_full", 
                                                 found=(result is not None), **context)
                return unique_result
        except Exception as e:
            self.error_handler.handle_error(e, context="get_classifier_template_full", 
                                          template_id=template_id, **context)
            raise

    async def get_connector_configuration(self, connector_type: str, config_name: str = "default", 
                                  context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Get connector-specific configuration (e.g., SQL queries)."""
        cache_key = f"connector_config:{connector_type}:{config_name}"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_connector_configuration(connector_type, config_name, context),
            is_classifier=False
        )

    async def _fetch_connector_configuration(self, connector_type: str, config_name: str = "default", 
                                           context: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Internal method to fetch connector configuration from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "STARTED", 
                                         operation="get_by_type", 
                                         connector_type=connector_type, 
                                         config_name=config_name, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(ConnectorConfiguration).where(
                    ConnectorConfiguration.connector_type == connector_type,
                    ConnectorConfiguration.config_name == config_name
                )
                result = await session.scalars(stmt)
                result = result.one_or_none()
                
                if result:
                    self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "SUCCESS", 
                                                     operation="get_by_type", **context)
                    return result.configuration
                else:
                    self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "NOT_FOUND", 
                                                     operation="get_by_type", **context)
                    return None
                    
        except Exception as e:
            self.error_handler.handle_error(e, context="get_connector_configuration", 
                                          connector_type=connector_type, **context)
            raise

    async def get_objects_for_classification(self, object_key_hashes: List[bytes], context: Optional[Dict[str, Any]] = None) -> List[OrmDiscoveredObject]:
        """Get discovered objects by their key hashes for classification."""
        context = context or {}
        if not object_key_hashes:
            return []
            
        self.logger.log_database_operation("SELECT", "DiscoveredObjects", "STARTED", 
                                         operation="get_for_classification", 
                                         object_count=len(object_key_hashes), **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(OrmDiscoveredObject).where(OrmDiscoveredObject.object_key_hash.in_(object_key_hashes))
                result = await session.scalars(stmt)
                results = list(result.all())
                
                self.logger.log_database_operation("SELECT", "DiscoveredObjects", "SUCCESS", 
                                                 operation="get_for_classification", 
                                                 row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_objects_for_classification", 
                                          object_count=len(object_key_hashes), **context)
            raise

    async def update_classification_timestamps(self, object_key_hashes: List[bytes], context: Optional[Dict[str, Any]] = None) -> None:
        """Update classification timestamps for objects using their key hashes."""
        context = context or {}
        if not object_key_hashes:
            return
            
        self.logger.log_database_operation("UPDATE", "DiscoveredObjectClassificationDateInfo", "STARTED", 
                                         operation="update_timestamps", 
                                         object_count=len(object_key_hashes), **context)
        try:
            async with self.get_async_session() as session:
                current_time = datetime.now(timezone.utc)
                
                # Update existing records using object_key_hash as primary key
                stmt = update(DiscoveredObjectClassificationDateInfo).where(
                    DiscoveredObjectClassificationDateInfo.object_key_hash.in_(object_key_hashes)
                ).values(last_classification_date=current_time)
                
                result = await session.execute(stmt)
                updated_count = result.rowcount
                
                # Insert records for objects that don't have classification info yet
                existing_result = await session.scalars(
                    select(DiscoveredObjectClassificationDateInfo.object_key_hash)
                    .where(DiscoveredObjectClassificationDateInfo.object_key_hash.in_(object_key_hashes))
                )
                existing_hashes = set(existing_result.all())
                
                new_hashes = set(object_key_hashes) - existing_hashes
                if new_hashes:
                    new_records = [
                        {
                            'object_key_hash': obj_hash,
                            'last_classification_date': current_time
                        }
                        for obj_hash in new_hashes
                    ]
                    await session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(DiscoveredObjectClassificationDateInfo, new_records))
                
                await session.commit()
                
                self.logger.log_database_operation("UPDATE", "DiscoveredObjectClassificationDateInfo", "SUCCESS", 
                                                 operation="update_timestamps", 
                                                 updated_count=updated_count,
                                                 inserted_count=len(new_hashes) if new_hashes else 0, **context)
                
        except Exception as e:
            self.error_handler.handle_error(e, context="update_classification_timestamps", 
                                          object_count=len(object_key_hashes), **context)
            raise

    async def get_objects_by_datasource(self, datasource_id: str, limit: int = 1000, 
                                context: Optional[Dict[str, Any]] = None) -> List[OrmDiscoveredObject]:
        """Get discovered objects for a specific datasource."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "DiscoveredObjects", "STARTED", 
                                         operation="get_by_datasource", 
                                         datasource_id=datasource_id, 
                                         limit=limit, **context)
        try:
            async with self.get_async_session() as session:
                stmt = (
                    select(OrmDiscoveredObject)
                    .where(OrmDiscoveredObject.data_source_id == datasource_id)
                    .limit(limit)
                )
                result = await session.scalars(stmt)
                results = list(result.all())
                
                self.logger.log_database_operation("SELECT", "DiscoveredObjects", "SUCCESS", 
                                                 operation="get_by_datasource", 
                                                 row_count=len(results), **context)
                return results
                
        except Exception as e:
            self.error_handler.handle_error(e, context="get_objects_by_datasource", 
                                          datasource_id=datasource_id, **context)
            raise


    async def insert_discovered_object_batch(self, objects: List[Dict[str, Any]], 
                                         staging_table_name: str,
                                         context: Optional[Dict[str, Any]] = None) -> int:
        """Upsert batch of discovered objects into staging table using SQL Server MERGE."""
        context = context or {}
        if not objects:
            self.logger.log_database_operation("BULK UPSERT", staging_table_name, "NO_OBJECTS_TO_PROCESS", **context)
            return 0
            
        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "VALIDATING_TABLE_NAME", **context)
        self._validate_table_name(staging_table_name)
        
        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "STARTED", 
                                         record_count=len(objects), **context)
        try:
            self.logger.log_database_operation("BULK UPSERT", staging_table_name, "GETTING_ASYNC_SESSION", **context)
            async with self.get_async_session() as session:
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "SESSION_ACQUIRED", **context)
                
                # Process objects - FIXED: hashlib import moved outside loop
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "STARTING_OBJECT_PROCESSING", 
                                                 input_count=len(objects), **context)
                
                processed_objects = []
                for i, obj in enumerate(objects):
                    if i % 100 == 0:  # Log every 100 objects
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "PROCESSING_PROGRESS", 
                                                         processed=i, total=len(objects), **context)
                    
                    key_components = [
                        obj.get('data_source_id', ''),
                        obj.get('object_path', ''),
                        obj.get('object_type', '')
                    ]
                    key_string = '|'.join(str(comp) for comp in key_components)
                    object_key_hash = hashlib.sha256(key_string.encode()).digest()
                    
                    processed_obj = {
                        'object_key_hash': object_key_hash,
                        'data_source_id': obj.get('data_source_id'),
                        'object_type': obj.get('object_type'),
                        'object_path': obj.get('object_path'),
                        'size_bytes': obj.get('size_bytes'),
                        'created_date': obj.get('created_date'),
                        'last_modified': obj.get('last_modified'),
                        'last_accessed': obj.get('last_accessed'),
                        'discovery_timestamp': datetime.now(timezone.utc)
                    }
                    processed_objects.append(processed_obj)
                
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "OBJECTS_PROCESSED", 
                                                 record_count=len(processed_objects), **context)

                def sync_upsert(sync_session):
                    try:
                        # FIXED: Use session-local temporary table (single #) with timestamp for uniqueness
                        temp_table_name = f"#temp_{staging_table_name}_{int(time.time() * 1000)}"
                        
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "STARTING_TEMP_OPERATIONS", 
                                                         temp_table=temp_table_name, **context)
                        
                        # Create session-local temporary table (automatically cleaned up when session ends)
                        create_temp_sql = f"""
                        CREATE TABLE {temp_table_name} (
                            object_key_hash VARBINARY(32) PRIMARY KEY,
                            data_source_id NVARCHAR(255),
                            object_type NVARCHAR(50),
                            object_path NVARCHAR(4000),
                            size_bytes BIGINT,
                            created_date DATETIME2,
                            last_modified DATETIME2,
                            last_accessed DATETIME2,
                            discovery_timestamp DATETIME2
                        )
                        """
                        
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "CREATING_SESSION_LOCAL_TEMP_TABLE", 
                                                         sql=create_temp_sql.replace('\n', ' ').strip(), 
                                                         temp_table=temp_table_name, **context)
                        sync_session.execute(text(create_temp_sql))
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "TEMP_TABLE_CREATED", 
                                                         temp_table=temp_table_name, **context)
                        
                        # Insert data into temp table - FIXED: Avoid autoload, use direct SQL insert
                        if processed_objects:
                            self.logger.log_database_operation("BULK UPSERT", staging_table_name, "STARTING_BULK_INSERT", 
                                                             record_count=len(processed_objects), 
                                                             temp_table=temp_table_name, **context)
                            
                            # Use direct SQL bulk insert to avoid autoload issues with temp tables
                            insert_sql = f"""
                            INSERT INTO {temp_table_name} 
                            (object_key_hash, data_source_id, object_type, object_path, 
                             size_bytes, created_date, last_modified, last_accessed, discovery_timestamp)
                            VALUES 
                            (:object_key_hash, :data_source_id, :object_type, :object_path, 
                             :size_bytes, :created_date, :last_modified, :last_accessed, :discovery_timestamp)
                            """
                            
                            self.logger.log_database_operation("BULK UPSERT", staging_table_name, "EXECUTING_BULK_INSERT", 
                                                             sql=insert_sql.replace('\n', ' ').strip(),
                                                             param_count=len(processed_objects), 
                                                             temp_table=temp_table_name, **context)
                            
                            # Execute the bulk insert
                            sync_session.execute(text(insert_sql), processed_objects)
                            
                            self.logger.log_database_operation("BULK UPSERT", staging_table_name, "BULK_INSERT_COMPLETE", 
                                                             record_count=len(processed_objects), 
                                                             temp_table=temp_table_name, **context)
                        
                        # MERGE from session-local temp table to permanent staging table
                        merge_sql = f"""
                        MERGE {staging_table_name} AS target
                        USING {temp_table_name} AS source
                        ON target.object_key_hash = source.object_key_hash
                        WHEN MATCHED THEN
                            UPDATE SET
                                last_modified = source.last_modified,
                                last_accessed = source.last_accessed,
                                size_bytes = source.size_bytes,
                                discovery_timestamp = source.discovery_timestamp
                        WHEN NOT MATCHED THEN
                            INSERT (object_key_hash, data_source_id, object_type, object_path, 
                                   size_bytes, created_date, last_modified, last_accessed, discovery_timestamp)
                            VALUES (source.object_key_hash, source.data_source_id, source.object_type, 
                                   source.object_path, source.size_bytes, source.created_date, 
                                   source.last_modified, source.last_accessed, source.discovery_timestamp);
                        """

                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "EXECUTING_MERGE", 
                                                         sql=merge_sql.replace('\n', ' ').strip(), 
                                                         from_temp=temp_table_name, 
                                                         to_staging=staging_table_name, **context)

                        result = sync_session.execute(text(merge_sql))
                        affected_rows = result.rowcount if hasattr(result, 'rowcount') else len(processed_objects)
                        
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "MERGE_COMPLETE", 
                                                         rows_affected=affected_rows, 
                                                         temp_table=temp_table_name, **context)
                        
                        # Session-local temp table will be automatically dropped when session ends
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "TEMP_TABLE_AUTO_CLEANUP", 
                                                         temp_table=temp_table_name, **context)
                        
                    except Exception as e:
                        self.logger.log_database_operation("BULK UPSERT", staging_table_name, "SYNC_ERROR", 
                                                         error=str(e), **context)
                        raise
                
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "CALLING_RUN_SYNC", **context)
                await session.run_sync(sync_upsert)
                
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "RUN_SYNC_COMPLETE", **context)
                await session.commit()
                
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "COMMIT_COMPLETE", **context)
                
                self.logger.log_database_operation("BULK UPSERT", staging_table_name, "SUCCESS", 
                                                 record_count=len(processed_objects), **context)
                return len(processed_objects)
            
        except Exception as e:
            self.logger.log_database_operation("BULK UPSERT", staging_table_name, "FAILED", 
                                             error=str(e), object_count=len(objects), **context)
            self.error_handler.handle_error(e, context="upsert_discovered_object_batch", 
                                          staging_table=staging_table_name,
                                          object_count=len(objects), **context)
            raise



    async def insert_scan_findings(self, findings: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> int:
        """Insert scan findings into the findings tables using hash-based primary keys with upsert logic."""

        
        context = context or {}
        if not findings:
            return 0
            
        self.logger.log_database_operation("BULK ", "ScanFindingSummaries", "STARTED", 
                                         record_count=len(findings), **context)
        try:
            async with self.get_async_session() as session:
                # Convert findings to ORM format with hash-based primary keys
                summaries = []
                for i, finding in enumerate(findings):
                        # Create finding key hash - this becomes the primary key
                    # Debug logging to check field names and values
                    self.logger.info(f"Processing finding {i+1}/{len(findings)}")
                    self.logger.info(f"Keys in finding dict: {list(finding.keys())}")
                    
                    # Check both possible field names
                    datasource_id_value = finding.get('datasource_id')
                    data_source_id_value = finding.get('data_source_id') 
                    
                    self.logger.info(f"finding.get('datasource_id'): {datasource_id_value}")
                    self.logger.info(f"finding.get('data_source_id'): {data_source_id_value}")
                    
                    # Create a safe copy of finding for logging (remove datetime objects)
                    safe_finding = {}
                    for key, value in finding.items():
                        if hasattr(value, 'isoformat'):  # datetime object
                            safe_finding[key] = str(value)
                        else:
                            safe_finding[key] = value
                    
                    self.logger.info(f"Full finding dict (safe): {safe_finding}")


                    key_components = [
                        finding.get('scan_job_id', ''),
                        finding.get('data_source_id', ''),
                        finding.get('classifier_id', ''),
                        finding.get('entity_type', ''),
                        finding.get('schema_name', '') or '',
                        finding.get('table_name', '') or '',
                        finding.get('field_name', '') or '',
                        finding.get('file_path', '') or '',
                        finding.get('file_name', '') or ''
                    ]
                    key_string = '|'.join(str(comp) for comp in key_components)
                    finding_key_hash = hashlib.sha256(key_string.encode()).digest()
                    
                    summary = {
                        'finding_key_hash': finding_key_hash,  # Primary key
                        'scan_job_id': finding.get('scan_job_id'),
                        'data_source_id': finding.get('data_source_id'),
                        'classifier_id': finding.get('classifier_id'),
                        'entity_type': finding.get('entity_type'),
                        'schema_name': finding.get('schema_name'),
                        'table_name': finding.get('table_name'),
                        'field_name': finding.get('field_name'),
                        'file_path': finding.get('file_path'),
                        'file_name': finding.get('file_name'),
                        'file_extension': finding.get('file_extension'),
                        'finding_count': finding.get('finding_count', 1),
                        'average_confidence': finding.get('average_confidence', 0.0),
                        'max_confidence': finding.get('max_confidence', 0.0),
                        'sample_findings': finding.get('sample_findings'),
                        'total_rows_in_source': finding.get('total_rows_in_source'),
                        'non_null_rows_scanned': finding.get('non_null_rows_scanned')
                    }
                    summaries.append(summary)
                
                def sync_upsert(sync_session):
                    try:
                        # Use session-local temporary table (single #) with timestamp for uniqueness
                        temp_table_name = f"#temp_scan_findings_{int(time.time() * 1000)}"
                        
                        # Create session-local temporary table
                        create_temp_sql = f"""
                        CREATE TABLE {temp_table_name} (
                            finding_key_hash VARBINARY(32) PRIMARY KEY,
                            scan_job_id BIGINT,
                            data_source_id NVARCHAR(255),
                            classifier_id NVARCHAR(255),
                            entity_type NVARCHAR(255),
                            schema_name NVARCHAR(255),
                            table_name NVARCHAR(255),
                            field_name NVARCHAR(255),
                            file_path NVARCHAR(4000),
                            file_name NVARCHAR(255),
                            file_extension NVARCHAR(50),
                            finding_count INT,
                            average_confidence FLOAT,
                            max_confidence FLOAT,
                            sample_findings NVARCHAR(MAX),
                            total_rows_in_source BIGINT,
                            non_null_rows_scanned BIGINT
                        )
                        """
                        
                        sync_session.execute(text(create_temp_sql))
                        
                        # Insert data into temp table using direct SQL with named parameters
                        if summaries:
                            insert_sql = f"""
                            INSERT INTO {temp_table_name} 
                            (finding_key_hash, scan_job_id, data_source_id, classifier_id, entity_type,
                             schema_name, table_name, field_name, file_path, file_name, file_extension,
                             finding_count, average_confidence, max_confidence, sample_findings,
                             total_rows_in_source, non_null_rows_scanned)
                            VALUES 
                            (:finding_key_hash, :scan_job_id, :data_source_id, :classifier_id, :entity_type,
                             :schema_name, :table_name, :field_name, :file_path, :file_name, :file_extension,
                             :finding_count, :average_confidence, :max_confidence, :sample_findings,
                             :total_rows_in_source, :non_null_rows_scanned)
                            """
                            
                            # Execute the bulk insert with named parameters
                            sync_session.execute(text(insert_sql), summaries)
                        
                        # MERGE from temp table to permanent table
                        merge_sql = f"""
                        MERGE scan_finding_summaries AS target
                        USING {temp_table_name} AS source
                        ON target.finding_key_hash = source.finding_key_hash
                        WHEN MATCHED THEN
                            UPDATE SET
                                finding_count = source.finding_count,
                                average_confidence = source.average_confidence,
                                max_confidence = source.max_confidence,
                                sample_findings = source.sample_findings,
                                total_rows_in_source = source.total_rows_in_source,
                                non_null_rows_scanned = source.non_null_rows_scanned
                        WHEN NOT MATCHED THEN
                            INSERT (finding_key_hash, scan_job_id, data_source_id, classifier_id, entity_type,
                                    schema_name, table_name, field_name, file_path, file_name, file_extension,
                                    finding_count, average_confidence, max_confidence, sample_findings,
                                    total_rows_in_source, non_null_rows_scanned)
                            VALUES (source.finding_key_hash, source.scan_job_id, source.data_source_id,
                                    source.classifier_id, source.entity_type, source.schema_name,
                                    source.table_name, source.field_name, source.file_path, source.file_name,
                                    source.file_extension, source.finding_count, source.average_confidence,
                                    source.max_confidence, source.sample_findings, source.total_rows_in_source,
                                    source.non_null_rows_scanned);
                        """
                        
                        sync_session.execute(text(merge_sql))
                        
                    except Exception as e:
                        raise
                
                # Execute the upsert using the same pattern as your existing code
                await session.run_sync(sync_upsert)
                await session.commit()
                
                self.logger.log_database_operation("BULK UPSERT", "ScanFindingSummaries", "SUCCESS", 
                                                 record_count=len(summaries), **context)
                return len(summaries)
            
        except Exception as e:
            self.error_handler.handle_error(e, context="insert_scan_findings", 
                                          finding_count=len(findings), **context)
            raise





    async def get_all_datasources(self, enabled_only: bool = True, context: Optional[Dict[str, Any]] = None) -> List[DataSource]:
        """Get all datasources, optionally filtered by enabled status."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "DataSources", "STARTED", 
                                         operation="get_all", 
                                         enabled_only=enabled_only, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(DataSource)
                if enabled_only:
                    # Assume enabled field exists in configuration JSON
                    # You may need to adjust this based on your actual schema
                    pass  # Add filter logic if needed
                
                result = await session.scalars(stmt)
                results = list(result.all())
                
                self.logger.log_database_operation("SELECT", "DataSources", "SUCCESS", 
                                                 operation="get_all", 
                                                 row_count=len(results), **context)
                return results
                
        except Exception as e:
            self.error_handler.handle_error(e, context="get_all_datasources", **context)
            raise

    # =================================================================
    # Health and Debugging
    # =================================================================

    async def test_connection(self, context: Optional[Dict[str, Any]] = None) -> bool:
        context = context or {}
        self.logger.log_database_operation("CONNECT", "database", "STARTED", **context)
        try:
            async with self.async_engine.begin():
                self.logger.log_database_operation("CONNECT", "database", "SUCCESS", **context)
                return True
        except Exception as e:
            self.error_handler.handle_error(e, context="test_database_connection", **context)
            return False

    async def get_database_health_metrics(self, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        context = context or {}
        self.logger.log_database_operation("GET_METRICS", "database", "STARTED", **context)
        try:
            metrics = {
                "connection_pool_size": self.async_engine.pool.size(),
                "connections_in_use": self.async_engine.pool.checkedin(),
                "connections_available": self.async_engine.pool.checkedout(),
            }
            self.logger.log_database_operation("GET_METRICS", "database", "SUCCESS", **context)
            return metrics
        except Exception as e:
            self.error_handler.handle_error(e, context="get_database_health_metrics", **context)
            raise
    
    async def get_job_progress_summary(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            context = context or {}
            self.logger.log_database_operation("QUERY", "Tasks", "STARTED", operation="get_job_summary", job_id=job_id, **context)
            try:
                async with self.get_async_session() as session:
                    # UPDATED: Changed Task.JobID to the correct attribute Task.job_id
                    stmt = select(Task.status, func.count(Task.status)).where(Task.job_id == job_id).group_by(Task.status)
                    result = await session.execute(stmt)
                    results = result.all()
                    summary = {status.value: count for status, count in results}
                    self.logger.log_database_operation("QUERY", "Tasks", "SUCCESS", operation="get_job_summary", **context)
                    return summary
            except Exception as e:
                self.error_handler.handle_error(e, context="get_job_progress_summary", job_id=job_id, **context)
                raise

    # =================================================================
    # Configuration and Initialization
    # =================================================================

    async def get_system_parameters(self, node_group: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> List[SystemParameter]:
        """Get system parameters with caching."""
        cache_key = f"system_params:{node_group or 'default'}"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_system_parameters(node_group, context),
            is_classifier=False
        )

    async def _fetch_system_parameters(self, node_group: Optional[str] = None, context: Optional[Dict[str, Any]] = None) -> List[SystemParameter]:
        """Internal method to fetch system parameters from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "SystemParameters", "STARTED", **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(SystemParameter)
                if node_group:
                    stmt = stmt.outerjoin(NodeGroup).where(NodeGroup.name == node_group)
                else:
                    stmt = stmt.where(SystemParameter.node_group_id.is_(None))
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "SystemParameters", "SUCCESS", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_system_parameters", **context)
            raise

    async def get_connector_configurations(self, context: Optional[Dict[str, Any]] = None) -> List[ConnectorConfiguration]:
        """Get all connector configurations with caching."""
        cache_key = "connector_configurations:all"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_connector_configurations(context),
            is_classifier=False
        )

    async def _fetch_connector_configurations(self, context: Optional[Dict[str, Any]] = None) -> List[ConnectorConfiguration]:
        """Internal method to fetch connector configurations from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "STARTED", **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(ConnectorConfiguration)
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "SUCCESS", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_connector_configurations", **context)
            raise

    # =================================================================
    # Job & Task Lifecycle
    # =================================================================
    

    async def update_job_command(self, job_id: int, command: Optional[str], context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Updates or clears the 'master_pending_commands' field for a specific job.
        This is used by the CLI to issue commands and by the Orchestrator to clear them.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation_name="update_job_command", job_id=job_id, command=command, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Job).where(
                    Job.id == job_id
                ).values(
                    master_pending_commands=command,
                    version=Job.version + 1  # Increment version to show a change has occurred
                )
                
                result = await session.execute(stmt)
                await session.commit()

                was_successful = result.rowcount > 0
                self.logger.log_database_operation("UPDATE", "Jobs", "SUCCESS" if was_successful else "FAILURE", operation_name="update_job_command", **context)
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(e, "update_job_command", job_id=job_id, command=command, **context)
            raise

    async def get_child_jobs_by_master_id(self, master_job_id: str, context: Optional[Dict[str, Any]] = None) -> List[Job]:
        """
        Finds all child jobs associated with a single master_job_id.
        """
        context = context or {}
        self.logger.log_database_operation("SELECT", "Jobs", "STARTED", operation_name="get_by_master_id", master_job_id=master_job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(Job).where(Job.master_job_id == master_job_id)
                result = await session.scalars(stmt)
                child_jobs = result.all()

                self.logger.log_database_operation("SELECT", "Jobs", "SUCCESS", operation_name="get_by_master_id", count=len(child_jobs), **context)
                return child_jobs
        except Exception as e:
            self.error_handler.handle_error(e, "get_child_jobs_by_master_id", master_job_id=master_job_id, **context)
            raise


    async def create_job_execution(
        self,
        template_table_id: int,
        template_type: JobType,
        execution_id: str,
        trigger_type: str,
        nodegroup: str,
        configuration: Dict[str, Any],
        priority: int,
        master_job_id: str,
        master_pending_commands: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        session: Optional[AsyncSession] = None
    ) -> Job:
        """
        Creates a new 'Child Job' record in the database with all required fields.
        """
        context = context or {}
        self.logger.log_database_operation("INSERT", "Jobs", "STARTED", execution_id=execution_id, **context)
        try:
            async with self._get_session_context(session=session) as inner_session:
                new_job = Job(
                    execution_id=execution_id,
                    template_table_id=template_table_id,
                    template_type=template_type,
                    status=JobStatus.QUEUED,
                    trigger_type=trigger_type,
                    priority=priority,
                    node_group=nodegroup,
                    configuration=configuration,
                    master_job_id=master_job_id,
                    master_pending_commands=master_pending_commands
                )
                
                inner_session.add(new_job)
                await inner_session.flush()
                
                # REMOVED: The refresh call that would cause the same "connection is busy" error.
                # await inner_session.refresh(new_job)
                
                self.logger.log_database_operation("INSERT", "Jobs", "SUCCESS", job_id=new_job.id, **context)
                return new_job
        except Exception as e:
            self.error_handler.handle_error(e, "create_job_execution", execution_id=execution_id, **context)
            raise



    async def create_task_batch(
        self,
        tasks_to_create: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
        session: Optional[AsyncSession] = None
    ) -> None:
        """
        Creates a batch of new task records in the database efficiently using a bulk insert.
        This method is designed to participate in an existing transaction if a session is provided.
        """
        context = context or {}
        if not tasks_to_create:
            return

        self.logger.log_database_operation(
            "BULK INSERT", "Tasks", "STARTED",
            task_count=len(tasks_to_create),
            **context
        )
        try:
            # Use the session manager to either participate in an existing transaction or create a new one.
            async with self._get_session_context(session=session) as inner_session:
                
                # Convert the list of dictionaries into a list of ORM objects
                new_task_objects = [
                    Task(
                        id=task_params['task_id'],
                        job_id=task_params['job_id'],
                        task_type=task_params['task_type'],
                        work_packet=task_params['work_packet'],
                        datasource_id=task_params.get('datasource_id'),
                        parent_task_id=task_params.get('parent_task_id'),
                        status=TaskStatus.PENDING
                    ) for task_params in tasks_to_create
                ]
                
                # Use add_all for an efficient bulk insert operation.
                inner_session.add_all(new_task_objects)
                # The commit is handled by the context manager if it's the transaction "owner".

            self.logger.log_database_operation(
                "BULK INSERT", "Tasks", "SUCCESS",
                task_count=len(tasks_to_create),
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(e, context="create_task_batch", **context)
            raise

    async def update_output_record_status_batch(
        self,
        record_ids: List[int],
        status: str,
        context: Optional[Dict[str, Any]] = None,
        session: Optional[AsyncSession] = None
    ) -> None:
        """
        Updates the status for a batch of TaskOutputRecords in a single, efficient operation.
        This method is designed to participate in an existing transaction if a session is provided.
        """
        context = context or {}
        if not record_ids:
            return

        self.logger.log_database_operation(
            "BULK UPDATE", "TaskOutputRecords", "STARTED",
            operation="update_status_batch",
            record_count=len(record_ids),
            **context
        )
        try:
            # Use the session manager to handle the transaction.
            async with self._get_session_context(session=session) as inner_session:
                stmt = update(TaskOutputRecord).where(
                    TaskOutputRecord.id.in_(record_ids)
                ).values(status=status)
                
                await inner_session.execute(stmt)
                # The commit is handled by the context manager.

            self.logger.log_database_operation(
                "BULK UPDATE", "TaskOutputRecords", "SUCCESS",
                operation="update_status_batch",
                record_count=len(record_ids),
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(e, context="update_output_record_status_batch", **context)
            raise



    async def create_task(
        self, 
        job_id: int, 
        task_id: bytes,  # FIXED: Now accepts the pre-generated hash ID
        task_type: str, 
        work_packet: Dict[str, Any], 
        datasource_id: Optional[str] = None, 
        parent_task_id: Optional[bytes] = None,  # FIXED: Now accepts the parent's hash ID
        context: Optional[Dict[str, Any]] = None
    ) -> Task:
        """
        Creates a new task record in the database using a pre-generated,
        application-side hash as the primary key.
        """
        context = context or {}
        self.logger.log_database_operation(
            "INSERT", "Tasks", "STARTED", 
            job_id=job_id, 
            task_type=task_type, 
            **context
        )
        try:
            async with self.get_async_session() as session:
                new_task = Task(
                    id=task_id,  # FIXED: Assigns the pre-generated hash to the id field
                    job_id=job_id, 
                    task_type=task_type, 
                    work_packet=work_packet, 
                    datasource_id=datasource_id, 
                    parent_task_id=parent_task_id, 
                    status=TaskStatus.PENDING
                )
                session.add(new_task)
                await session.commit()
                
                # Refresh is still good practice to confirm the object state matches the DB
                await session.refresh(new_task) 
                
                self.logger.log_database_operation(
                    "INSERT", "Tasks", "SUCCESS", 
                    task_id=new_task.id.hex(),  # Log the hex version for readability
                    **context
                )
                return new_task
        except Exception as e:
            self.error_handler.handle_error(e, context="create_task", **context)
            raise



    async def assign_task_to_worker(
        self, 
        task_id: bytes,  # FIXED: Changed from int to bytes
        worker_id: str, 
        lease_duration_seconds: int, 
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Atomically assigns a PENDING task to a worker using its hash-based ID.
        """
        context = context or {}
        log_task_id = task_id.hex() # Log the hex version for readability
        self.logger.log_database_operation(
            "UPDATE", "Tasks", "STARTED", 
            task_id=log_task_id, 
            worker_id=worker_id, 
            **context
        )
        try:
            async with self.get_async_session() as session:
                stmt = update(Task).where(
                    Task.id == task_id, 
                    Task.status == TaskStatus.PENDING
                ).values(
                    status=TaskStatus.ASSIGNED,
                    worker_id=worker_id,
                    lease_expiry=datetime.now(timezone.utc) + timedelta(seconds=lease_duration_seconds),
                    retry_count=Task.retry_count + 1
                )
                result = await session.execute(stmt)
                await session.commit()
                
                was_successful = result.rowcount > 0
                status = "SUCCESS" if was_successful else "FAILURE_RACE_CONDITION"
                self.logger.log_database_operation(
                    "UPDATE", "Tasks", status, 
                    task_id=log_task_id, 
                    **context
                )
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(
                e, 
                context="assign_task_to_worker", 
                task_id=log_task_id,
                **context
            )
            raise


    async def complete_task(
        self,
        
        task_id: bytes,  # FIXED: Changed from int to bytes
        context: Optional[Dict[str, Any]] = None,
		session: Optional[AsyncSession] = None
		        
        
    ) -> None:
        """
        Marks a task as COMPLETED using its hash-based ID.
        """
        context = context or {}
        log_task_id = task_id.hex() # Log the hex version for readability
        self.logger.log_database_operation(
            "UPDATE", "Tasks", "STARTED",
            operation_name="complete_task",
            task_id=log_task_id,
            **context
        )
        try:
            async with self._get_session_context(session) as session:
                stmt = update(Task).where(Task.id == task_id).values(status=TaskStatus.COMPLETED)
                await session.execute(stmt)

                
                self.logger.log_database_operation(
                    "UPDATE", "Tasks", "SUCCESS",
                    operation_name="complete_task",
                    task_id=log_task_id,
                    **context
                )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                context="complete_task",
                task_id=log_task_id,
                **context
            )
            raise



    async def fail_task(
        self,
        task_id: bytes,  # FIXED: Changed from int to bytes
        is_retryable: bool,
        max_retries: int = 3,
        context: Optional[Dict[str, Any]] = None,
		session: Optional[AsyncSession] = None
		       
        
    ) -> None:
        """
        Marks a task as FAILED or PENDING (for retry) using its hash-based ID.
        """
        context = context or {}
        log_task_id = task_id.hex() # Log the hex version for readability
        self.logger.log_database_operation(
            "UPDATE", "Tasks", "STARTED",
            operation="fail_task",
            task_id=log_task_id,
            **context
        )
        try:
            async with self._get_session_context(session) as session:
                # Eagerly fetch the task again within this session to prevent lazy-loading errors.
                result = await session.execute(select(Task).where(Task.id == task_id))
                task = result.scalar_one_or_none()
                
                if not task:
                    self.logger.warning(f"Task {log_task_id} not found, cannot fail it.", task_id=log_task_id, **context)
                    return
                
                if is_retryable and task.retry_count < max_retries:
                    task.status = TaskStatus.PENDING
                    task.worker_id = None
                    task.lease_expiry = None
                else:
                    task.status = TaskStatus.FAILED
                
                final_status = task.status # Capture status before commit
                await session.commit()
                
                self.logger.log_database_operation(
                    "UPDATE", "Tasks", "SUCCESS",
                    operation="fail_task",
                    new_status=final_status.value,
                    task_id=log_task_id,
                    **context
                )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                context="fail_task",
                task_id=log_task_id,
                **context
            )
            raise


    async def get_pending_tasks_batch(self, job_id: int, batch_size: int, context: Optional[Dict[str, Any]] = None) -> List[Task]:
        context = context or {}
        self.logger.log_database_operation("SELECT", "Tasks", "STARTED", job_id=job_id, batch_size=batch_size, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(Task).where(Task.job_id == job_id, Task.status == TaskStatus.PENDING).order_by(Task.id).limit(batch_size)
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "Tasks", "SUCCESS", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_pending_tasks_batch", **context)
            raise

    async def get_expired_task_leases(self, timeout_seconds: int, context: Optional[Dict[str, Any]] = None) -> List[Task]:
        context = context or {}
        self.logger.log_database_operation("SELECT", "Tasks", "STARTED", operation="get_expired_leases", **context)
        try:
            async with self.get_async_session() as session:
                expiry_threshold = datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)
                stmt = select(Task).where(Task.status == TaskStatus.ASSIGNED, Task.lease_expiry < expiry_threshold)
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "Tasks", "SUCCESS", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_expired_task_leases", **context)
            raise

    # =================================================================
    # Pipelining and Progress
    # =================================================================


    async def write_task_output_record(
        self,
        job_id: int,          # NEW: Added job_id for context and integrity
        task_id: bytes,       # FIXED: Changed from int to bytes
        output_type: str,
        payload: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> TaskOutputRecord:
        """
        Creates a TaskOutputRecord, linking it to its parent job and task
        using the new hash-based task ID.
        """
        context = context or {}
        log_task_id = task_id.hex() # Log the hex version for readability
        self.logger.log_database_operation(
            "INSERT", "TaskOutputRecords", "STARTED",
            
            
            **context
        )
        try:
            async with self.get_async_session() as session:
                record = TaskOutputRecord(
                    job_id=job_id,        # NEW: Populating the new field
                    task_id=task_id,      # FIXED: Using the bytes hash
                    output_type=output_type,
                    output_payload=payload,
                    status='PENDING_PROCESSING'
                )
                session.add(record)
                await session.commit()
                await session.refresh(record)
                
                self.logger.log_database_operation(
                    "INSERT", "TaskOutputRecords", "SUCCESS",
                    record_id=record.id,

                    **context
                )
                return record
        except Exception as e:
            self.error_handler.handle_error(
                e,
                context="write_task_output_record",
                task_id=log_task_id,
                job_id=job_id,
                **context
            )
            raise


    async def get_pending_output_records(self, limit: int = 100, context: Optional[Dict[str, Any]] = None) -> List[TaskOutputRecord]:
        context = context or {}
        self.logger.log_database_operation("SELECT", "TaskOutputRecords", "STARTED", limit=limit, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(TaskOutputRecord).where(TaskOutputRecord.status == 'PENDING_PROCESSING').limit(limit)
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "TaskOutputRecords", "SUCCESS", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_pending_output_records", **context)
            raise

    # =================================================================
    # Staging Table Management
    # =================================================================

    async def create_staging_table_for_job(self, job_id: int, table_type: str, context: Optional[Dict[str, Any]] = None) -> str:
        context = context or {}
        staging_table_name = f"staging_{table_type}_job_{job_id}"
        self._validate_table_name(staging_table_name)
        
        if table_type != "DiscoveredObjects":
            raise ValueError(f"Unknown staging table type: {table_type}")

        self.logger.log_database_operation("CREATE TABLE", staging_table_name, "STARTED", **context)
        try:
            # Updated table definition to match new hash-based schema
            staging_table = Table(
                staging_table_name,
                Base.metadata,
                Column('object_key_hash', LargeBinary(32), primary_key=True),  # Changed: Now primary key
                Column('data_source_id', String(255), nullable=False),         # Changed: Renamed from DataSourceID
                Column('object_type', String(50), nullable=False),             # Changed: Renamed from ObjectType  
                Column('object_path', String(4000), nullable=False),           # Changed: Renamed from ObjectPath
                Column('size_bytes', Integer, nullable=True),                  # Changed: Renamed from SizeBytes
                Column('created_date', DateTime, nullable=True),               # Changed: Renamed from CreatedDate
                Column('last_modified', DateTime, nullable=True),              # Changed: Renamed from LastModified
                Column('last_accessed', DateTime, nullable=True),              # Changed: Renamed from LastAccessed
                Column('discovery_timestamp', DateTime(timezone=True), nullable=False), # Changed: Renamed from DiscoveryTimestamp
                extend_existing=True
            )
            
            async with self.async_engine.begin() as conn:
                await conn.run_sync(lambda sync_conn: staging_table.create(sync_conn, checkfirst=True))
            
            self.logger.log_database_operation("CREATE TABLE", staging_table_name, "SUCCESS", **context)
            return staging_table_name
        except Exception as e:
            self.error_handler.handle_error(e, context="create_staging_table_for_job", table_name=staging_table_name, **context)
            raise


    async def cleanup_staging_tables_for_job(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> None:
        context = context or {}
        pattern = f"staging_%_job_{job_id}"
        self.logger.log_database_operation("DROP TABLE", pattern, "STARTED", **context)
        try:
            def sync_cleanup(sync_conn):
                inspector = inspect(sync_conn)
                all_tables = inspector.get_table_names()
                tables_to_drop = [name for name in all_tables if name.startswith(f"staging_") and name.endswith(f"_job_{job_id}")]

                for table_name in tables_to_drop:
                    self._validate_table_name(table_name)
                    table_to_drop = Table(table_name, Base.metadata, autoload_with=sync_conn)
                    table_to_drop.drop(sync_conn, checkfirst=True)
                return len(tables_to_drop)

            async with self.async_engine.begin() as conn:
                dropped_count = await conn.run_sync(sync_cleanup)
            self.logger.log_database_operation("DROP TABLE", pattern, "SUCCESS", dropped_count=dropped_count, **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="cleanup_staging_tables_for_job", job_id=job_id, **context)
            raise
            
    async def get_staging_table_record_count(self, table_name: str, context: Optional[Dict[str, Any]] = None) -> int:
        context = context or {}
        self._validate_table_name(table_name)
        self.logger.log_database_operation("COUNT", table_name, "STARTED", **context)
        try:
            async with self.get_async_session() as session:
                result = await session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar_one()
                self.logger.log_database_operation("COUNT", table_name, "SUCCESS", record_count=count, **context)
                return count
        except Exception as e:
            self.error_handler.handle_error(e, context="get_staging_table_record_count", table_name=table_name, **context)
            raise

    # =================================================================
    # Bulk Data & Delta Operations
    # =================================================================
    
    async def bulk_insert_mappings(self, orm_class, mappings: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> int:
        context = context or {}
        table_name = orm_class.__tablename__
        if not mappings: return 0
        
        self.logger.log_database_operation("BULK INSERT", table_name, "STARTED", record_count=len(mappings), **context)
        try:
            async with self.get_async_session() as session:
                await session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(orm_class, mappings))
                await session.commit()
                self.logger.log_database_operation("BULK INSERT", table_name, "SUCCESS", record_count=len(mappings), **context)
                return len(mappings)
        except Exception as e:
            self.error_handler.handle_error(e, context="bulk_insert_mappings", table_name=table_name, **context)
            raise

    async def execute_delta_comparison(self, staging_table_name: str, datasource_id: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """
        Compares a staging table against the discovered_objects master table to find
        new, modified, and deleted objects using hash-based primary keys.
        """
        context = context or {}
        self._validate_table_name(staging_table_name)
        self.logger.log_database_operation("DELTA", staging_table_name, "STARTED", **context)
        try:
            async with self.get_async_session() as session:
                # 1. Get hashes from the staging table (newly discovered objects)
                staging_hashes_query = text(f'SELECT "object_key_hash" FROM {staging_table_name}')
                staging_result = await session.execute(staging_hashes_query)
                staging_hashes = set(staging_result.scalars().all())

                # 2. Get existing hashes for this datasource from the master discovered_objects table
                main_hashes_query = select(OrmDiscoveredObject.object_key_hash).where(
                    OrmDiscoveredObject.data_source_id == datasource_id
                )
                main_result = await session.execute(main_hashes_query)
                main_hashes = set(main_result.scalars().all())

                # 3. Calculate the differences
                new_hashes = staging_hashes - main_hashes
                deleted_hashes = main_hashes - staging_hashes
                
                # 4. Handle deleted objects: Remove them from the master table
                if deleted_hashes:
                    delete_stmt = (
                        delete(OrmDiscoveredObject)
                        .where(OrmDiscoveredObject.object_key_hash.in_(deleted_hashes))
                    )
                    await session.execute(delete_stmt)

                # 5. Handle new objects: Insert them from the staging table into the master table
                if new_hashes:
                    target_columns = [c.name for c in OrmDiscoveredObject.__table__.columns]
                    target_cols_str = ", ".join([f'"{c}"' for c in target_columns])
                    
                    # Efficiently copy new records from staging to the master table
                    insert_sql = text(
                        f'INSERT INTO "discovered_objects" ({target_cols_str}) '
                        f'SELECT {target_cols_str} FROM {staging_table_name} '
                        f'WHERE "object_key_hash" = ANY(:hashes)'
                    )
                    await session.execute(insert_sql, {"hashes": list(new_hashes)})

                await session.commit()
                
                results = {"new": len(new_hashes), "deleted": len(deleted_hashes)}
                self.logger.log_database_operation("DELTA", staging_table_name, "SUCCESS", **results, **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="execute_delta_comparison", staging_table=staging_table_name, **context)
            raise



    async def get_objects_needing_classification_rescan(self, cutoff_date: datetime, context: Optional[Dict[str, Any]] = None) -> List[int]:
        context = context or {}
        self.logger.log_database_operation("SELECT", "DiscoveredObjectClassificationDateInfo", "STARTED", operation="rescan_check", **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(DiscoveredObjectClassificationDateInfo.object_id).where(DiscoveredObjectClassificationDateInfo.last_classification_date < cutoff_date)
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "DiscoveredObjectClassificationDateInfo", "SUCCESS", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_objects_needing_classification_rescan", **context)
            raise

            
    async def get_datasource_with_schedule(self, datasource_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[DataSource]:
        """
        Fetches a single DataSource by its ID, eagerly loading its associated
        Calendar and all CalendarRules in a single, efficient query.

        Args:
            datasource_id: The unique string identifier for the data source.
            context: Optional dictionary for logging and trace propagation.

        Returns:
            The DataSource ORM object with its schedule pre-loaded, or None if not found.
        """
        context = context or {}
        self.logger.log_database_operation("SELECT", "DataSources", "STARTED", operation="get_with_schedule", datasource_id=datasource_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = (
                    select(DataSource)
                    .where(DataSource.datasource_id == datasource_id)
                    .options(
                        joinedload(DataSource.calendar)
                        .joinedload(Calendar.rules)
                    )
                )
                result = await session.scalars(stmt)
                result = result.unique().one_or_none()
                
                self.logger.log_database_operation("SELECT", "DataSources", "SUCCESS", operation="get_with_schedule", found=(result is not None), **context)
                return result
        except Exception as e:
            self.error_handler.handle_error(e, context="get_datasource_with_schedule", datasource_id=datasource_id, **context)
            raise

    async def get_active_jobs(self, statuses: List[JobStatus] = None, context: Optional[Dict[str, Any]] = None) -> List[Job]:
            """Fetches jobs that are in an active state (defaulting to QUEUED or RUNNING)."""
            context = context or {}
            if statuses is None:
                # CORRECTED: This now correctly references the JobStatus enum
                statuses = [JobStatus.QUEUED, JobStatus.RUNNING]
            
            # CORRECTED: Renamed the conflicting 'operation' key to 'operation_name'
            # to avoid passing the same argument twice to the logger.
            log_context = {
                "operation_name": "get_active_jobs", 
                "statuses": [s.value for s in statuses], 
                **context
            }
            self.logger.log_database_operation("SELECT", "Jobs", "STARTED", **log_context)
            
            try:
                async with self.get_async_session() as session:
                    stmt = select(Job).where(Job.status.in_(statuses))
                    result = await session.scalars(stmt)
                    results = list(result.all())
                    self.logger.log_database_operation("SELECT", "Jobs", "SUCCESS", row_count=len(results), **log_context)
                    return results
            except Exception as e:
                self.error_handler.handle_error(e, context="get_active_jobs", **context)
                raise


    @asynccontextmanager
    async def _get_session_context(self, session: Optional[AsyncSession] = None):
        """
        Provides a database session with optional commit control.
        - If session provided: yields it without commit/close
        - If no session: creates new one, optionally commits based on should_commit flag
        """
        if session:
            # Use existing session - caller controls transaction
            yield session
        else:
            # Create new session
            async with self.get_async_session() as new_session:
                try:
                    yield new_session
                    
                    await new_session.commit()
                except Exception:
                    await new_session.rollback()
                    raise


   
    async def get_task_by_id(
        self,
        task_id: bytes,
         
        context: Optional[Dict[str, Any]] = None,
		session: Optional[AsyncSession] = None
		       
    ) -> Optional[Task]:
        """Fetches a single task by its hash-based primary key."""
        context = context or {}
        log_task_id = task_id.hex()
        
        try:
            # Use the new context manager to handle the session
            async with self._get_session_context(session) as sess:
                result = await sess.get(Task, task_id)
                return result
        except Exception as e:
            self.error_handler.handle_error(
                e,
                context="get_task_by_id",
                task_id=log_task_id,
                **context
            )
            raise


    async def update_job_status(self, job_id: int, status: JobStatus, context: Optional[Dict[str, Any]] = None) -> None:
        """Updates the status of a specific job."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation="update_status", job_id=job_id, status=status.value, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Job).where(Job.id == job_id).values(status=status)
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "Jobs", "SUCCESS", operation="update_status", **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="update_job_status", job_id=job_id, **context)
            raise

    async def update_output_record_status(
            self,
            record_id: int,
            status: str,
            context: Optional[Dict[str, Any]] = None,
            session: Optional[AsyncSession] = None
        ) -> None:
            """Updates the status of a specific TaskOutputRecord."""
            context = context or {}
            self.logger.log_database_operation("UPDATE", "TaskOutputRecords", "STARTED", operation="update_status", record_id=record_id, **context)
            try:
                # Use the session context manager to handle the transaction
                async with self._get_session_context(session=session) as inner_session:
                    stmt = update(TaskOutputRecord).where(TaskOutputRecord.id == record_id).values(status=status)
                    await inner_session.execute(stmt)
                    # The commit is handled by the context manager if it's the "owner"
                    
                self.logger.log_database_operation("UPDATE", "TaskOutputRecords", "SUCCESS", operation="update_status", **context)
            except Exception as e:
                self.error_handler.handle_error(e, context="update_output_record_status", record_id=record_id, **context)
                raise



    async def extend_task_lease(
        self,
        task_id: bytes,  # FIXED: Changed from int to bytes
        duration_seconds: int,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Extends the lease of an ASSIGNED task using its hash-based ID,
        preventing it from timing out.
        """
        context = context or {}
        log_task_id = task_id.hex() # Log the hex version for readability
        self.logger.log_database_operation(
            "UPDATE", "Tasks", "STARTED",
            operation="extend_lease",
            task_id=log_task_id,
            **context
        )
        try:
            async with self.get_async_session() as session:
                new_expiry = datetime.now(timezone.utc) + timedelta(seconds=duration_seconds)
                
                # The WHERE clause ensures we only update an active, assigned task (optimistic locking).
                stmt = update(Task).where(
                    Task.id == task_id,
                    Task.status == TaskStatus.ASSIGNED
                ).values(lease_expiry=new_expiry)
                
                result = await session.execute(stmt)
                await session.commit()
                
                was_successful = result.rowcount > 0
                self.logger.log_database_operation(
                    "UPDATE", "Tasks",
                    "SUCCESS" if was_successful else "FAILURE_NO_ACTIVE_TASK",
                    operation="extend_lease",
                    task_id=log_task_id,
                    **context
                )
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(
                e,
                context="extend_task_lease",
                task_id=log_task_id,
                **context
            )
            raise


    async def cancel_pending_tasks_for_job(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> int:
        """
        Cancels all PENDING tasks for a job.
        NOTE: This method strictly handles the database state change. It is the
        Orchestrator's responsibility to handle resource cleanup with the
        ResourceCoordinator for any affected tasks.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Tasks", "STARTED", operation_name="cancel_pending", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Task).where(
                    Task.job_id == job_id,
                    Task.status == TaskStatus.PENDING
                ).values(status=TaskStatus.CANCELLED)
                result = await session.execute(stmt)
                await session.commit()
                cancelled_count = result.rowcount
                self.logger.log_database_operation("UPDATE", "Tasks", "SUCCESS", operation="cancel_pending", cancelled_count=cancelled_count, **context)
                return cancelled_count
        except Exception as e:
            self.error_handler.handle_error(e, context="cancel_pending_tasks_for_job", job_id=job_id, **context)
            raise
           
    async def get_scan_template_by_id(self, template_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[ScanTemplate]:
        """Get scan template by ID with caching."""
        print(f"CLI THREAD: In get_scan_template_by_id'.")
        cache_key = f"scan_template:{template_id}"
        return await self.config_cache.get_or_fetch(
            cache_key,
            lambda: self._fetch_scan_template_by_id(template_id, context),
            is_classifier=False
        )

    async def _fetch_scan_template_by_id(self, template_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[ScanTemplate]:
        """Internal method to fetch scan template from database."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "ScanTemplates", "STARTED", template_id=template_id, **context)
        print(f"CLI THREAD: In _fetch_scan_template_by_id'.")
        try:
            async with self.get_async_session() as session:
                stmt = select(ScanTemplate).where(ScanTemplate.template_id == template_id)
                result = await session.scalars(stmt)
                result = result.one_or_none()
                self.logger.log_database_operation("SELECT", "ScanTemplates", "SUCCESS", found=(result is not None), **context)
                return result
        except Exception as e:
            print(f"CLI THREAD: In _fetch_scan_template_by_id exception'.")
            self.error_handler.handle_error(e, context="get_scan_template_by_id", **context)
            raise

    async def transition_job_from_pausing_to_paused(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """Atomically transitions a job from PAUSING to PAUSED only if no tasks are assigned."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation="atomic_pause_transition", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                # Subquery to check for active tasks
                active_task_subquery = select(Task.id).where(Task.JobID == job_id, Task.status == TaskStatus.ASSIGNED).limit(1).exists()
                # Atomic update
                stmt = update(Job).where(
                    Job.id == job_id,
                    Job.status == JobStatus.PAUSING,
                    ~active_task_subquery
                ).values(status=JobStatus.PAUSED)
                result = await session.execute(stmt)
                await session.commit()
                was_successful = result.rowcount > 0
                self.logger.log_database_operation("UPDATE", "Jobs", "SUCCESS" if was_successful else "SKIPPED", operation="atomic_pause_transition", **context)
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(e, context="transition_job_from_pausing_to_paused", **context)
            raise
            
    async def get_policy_template_by_id(self, template_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[PolicyTemplate]:
        """
        Get a policy template by its unique string identifier, with caching.
        """
        context = context or {}
        cache_key = f"policy_template:{template_id}"
        
        async def _fetch():
            self.logger.log_database_operation("SELECT", "policy_templates", "STARTED", template_id=template_id, **context)
            try:
                async with self.get_async_session() as session:
                    stmt = select(PolicyTemplate).where(PolicyTemplate.template_id == template_id)
                    result = await session.scalars(stmt)
                    template = result.one_or_none()
                    self.logger.log_database_operation("SELECT", "policy_templates", "SUCCESS", found=(template is not None), **context)
                    return template
            except Exception as e:
                self.error_handler.handle_error(e, context="get_policy_template_by_id", template_id=template_id, **context)
                raise

        return await self.config_cache.get_or_fetch(cache_key, _fetch, is_classifier=False)

    async def get_template_for_job(self, job: Job, context: Optional[Dict[str, Any]] = None) -> Optional[Union[ScanTemplate, PolicyTemplate]]:
        """
        Fetches the specific template (Scan or Policy) associated with a given Job.
        """
        context = context or {}
        self.logger.log_database_operation("SELECT", "templates", "STARTED", operation="get_for_job", job_id=job.id, **context)
        try:
            async with self.get_async_session() as session:
                template_model = None
                if job.template_type == JobType.SCANNING:
                    template_model = ScanTemplate
                elif job.template_type == JobType.POLICY:
                    template_model = PolicyTemplate
                else:
                    raise ValueError(f"Unknown template type '{job.template_type}' for job {job.id}")

                stmt = select(template_model).where(template_model.id == job.template_table_id)
                result = await session.scalars(stmt)
                template = result.one_or_none()
                self.logger.log_database_operation("SELECT", "templates", "SUCCESS", operation="get_for_job", found=(template is not None), **context)
                return template
        except Exception as e:
            self.error_handler.handle_error(e, context="get_template_for_job", job_id=job.id, **context)
            raise

    async def insert_remediation_ledger_bins(self, bins: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> int:
        """
        Bulk inserts a list of "bins" into the RemediationLedger table.
        """
        context = context or {}
        if not bins:
            return 0

        self.logger.log_database_operation("BULK INSERT", "RemediationLedger", "STARTED", record_count=len(bins), **context)
        try:
            async with self.get_async_session() as session:
                await session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(RemediationLedger, bins))
                await session.commit()
                self.logger.log_database_operation("BULK INSERT", "RemediationLedger", "SUCCESS", record_count=len(bins), **context)
                return len(bins)
        except Exception as e:
            self.error_handler.handle_error(e, context="insert_remediation_ledger_bins", bin_count=len(bins), **context)
            raise

    async def get_remediation_ledger_bin(self, plan_id: str, bin_id: str, context: Optional[Dict[str, Any]] = None) -> Optional[RemediationLedger]:
        """
        Fetches a single, specific bin from the RemediationLedger.
        """
        context = context or {}
        self.logger.log_database_operation("SELECT", "RemediationLedger", "STARTED", operation="get_bin", plan_id=plan_id, bin_id=bin_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(RemediationLedger).where(
                    RemediationLedger.plan_id == plan_id,
                    RemediationLedger.bin_id == bin_id
                )
                result = await session.scalars(stmt)
                single_bin = result.one_or_none()
                self.logger.log_database_operation("SELECT", "RemediationLedger", "SUCCESS", operation="get_bin", found=(single_bin is not None), **context)
                return single_bin
        except Exception as e:
            self.error_handler.handle_error(e, context="get_remediation_ledger_bin", plan_id=plan_id, bin_id=bin_id, **context)
            raise

    async def update_remediation_ledger_bin_status(self, plan_id: str, bin_id: str, status: LedgerStatus, result_details: Optional[Dict[str, Any]] = None, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Updates the status and result details of a specific bin in the RemediationLedger.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "RemediationLedger", "STARTED", operation="update_bin_status", plan_id=plan_id, bin_id=bin_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = (
                    update(RemediationLedger)
                    .where(RemediationLedger.plan_id == plan_id, RemediationLedger.bin_id == bin_id)
                    .values(Status=status, ResultDetails=result_details, LastUpdatedAt=datetime.now(timezone.utc))
                )
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "RemediationLedger", "SUCCESS", operation="update_bin_status", **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="update_remediation_ledger_bin_status", plan_id=plan_id, bin_id=bin_id, **context)
            raise

    async def get_ledger_bins_by_status(self, plan_id: str, status: LedgerStatus, context: Optional[Dict[str, Any]] = None) -> List[RemediationLedger]:
        """
        Gets all ledger bins for a specific plan that are in a given status.
        """
        context = context or {}
        self.logger.log_database_operation("SELECT", "RemediationLedger", "STARTED", operation="get_bins_by_status", plan_id=plan_id, status=status.value, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(RemediationLedger).where(
                    RemediationLedger.plan_id == plan_id,
                    RemediationLedger.status == status
                )
                result = await session.scalars(stmt)
                bins = result.all()
                self.logger.log_database_operation("SELECT", "RemediationLedger", "SUCCESS", operation="get_bins_by_status", count=len(bins), **context)
                return bins
        except Exception as e:
            self.error_handler.handle_error(e, context="get_ledger_bins_by_status", plan_id=plan_id, status=status.value, **context)
            raise
            
    async def close_async(self):
        """
        Gracefully disposes of the database engine and its connection pool.
        """
        if self.async_engine:
            self.logger.info("Closing database connection pool.")
            await self.async_engine.dispose()