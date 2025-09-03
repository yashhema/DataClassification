# src/core/db/database_interface.py
"""
Provides a high-level, unified, and production-ready interface for all 
database operations. This class is fully integrated with the system's
logging and error handling frameworks.

UPDATED: Added missing methods required for connector integration.
UPDATED: Converted to async/await pattern with configuration caching.
"""

import re
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from sqlalchemy.orm import joinedload

from sqlalchemy import (
    select, update, text, inspect, Table, MetaData, func, delete, Column,
    Integer, String, DateTime, LargeBinary
)
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.exc import SQLAlchemyError

# Import all ORM models from schema definitions
from ..db_models.base import Base
from ..db_models.job_schema import ScanTemplate, Job, Task, TaskStatus, TaskOutputRecord
from ..db_models.discovery_catalog_schema import DiscoveredObject as OrmDiscoveredObject, ObjectMetadata
from ..db_models.discovery_catalog_schema import DiscoveredObjectClassificationDateInfo
from ..db_models.findings_schema import ScanFindingSummary, ScanFindingOccurrence
from ..db_models.system_parameters_schema import SystemParameter
from ..db_models.datasource_schema import NodeGroup, DataSource
from ..db_models.connector_config_schema import ConnectorConfiguration
from ..db_models.calendar_schema import Calendar
from ..db_models.job_schema import JobStatus
from ..db_models.credentials_schema import Credential
from ..db_models.classifiertemplate_schema import ClassifierTemplate
from ..db_models.classifier_schema import Classifier

# Import Pydantic models for type hinting and data conversion
from ..models.models import DiscoveredObject as PydanticDiscoveredObject
from ..models.models import PIIFinding as PydanticPIIFinding

# Import core services for integration
from ..logging.system_logger import SystemLogger
from ..errors import ErrorHandler


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
                        .joinedload(Classifier.validation_rules)
                    )
                )
                result = await session.scalars(stmt)
                result = result.one_or_none()
                
                self.logger.log_database_operation("SELECT", "ClassifierTemplates", "SUCCESS", 
                                                 operation="get_full", 
                                                 found=(result is not None), **context)
                return result
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
                    ConnectorConfiguration.ConnectorType == connector_type,
                    ConnectorConfiguration.ConfigName == config_name
                )
                result = await session.scalars(stmt)
                result = result.one_or_none()
                
                if result:
                    self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "SUCCESS", 
                                                     operation="get_by_type", **context)
                    return result.Configuration
                else:
                    self.logger.log_database_operation("SELECT", "ConnectorConfigurations", "NOT_FOUND", 
                                                     operation="get_by_type", **context)
                    return None
                    
        except Exception as e:
            self.error_handler.handle_error(e, context="get_connector_configuration", 
                                          connector_type=connector_type, **context)
            raise

    async def insert_scan_findings(self, findings: List[Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> int:
        """Insert scan findings into the findings tables."""
        context = context or {}
        if not findings:
            return 0
            
        self.logger.log_database_operation("BULK INSERT", "ScanFindingSummaries", "STARTED", 
                                         record_count=len(findings), **context)
        try:
            async with self.get_async_session() as session:
                # Convert findings to ORM format
                summaries = []
                for finding in findings:
                    # Create finding key hash
                    import hashlib
                    key_components = [
                        finding.get('scan_job_id', ''),
                        finding.get('datasource_id', ''),
                        finding.get('classifier_id', ''),
                        finding.get('object_path', ''),
                        finding.get('field_name', '')
                    ]
                    key_string = '|'.join(str(comp) for comp in key_components)
                    finding_key_hash = hashlib.sha256(key_string.encode()).digest()
                    
                    summary = {
                        'FindingKeyHash': finding_key_hash,
                        'ScanJobID': finding.get('scan_job_id'),
                        'DataSourceID': finding.get('datasource_id'),
                        'ClassifierID': finding.get('classifier_id'),
                        'EntityType': finding.get('entity_type'),
                        'SchemaName': finding.get('schema_name'),
                        'TableName': finding.get('table_name'),
                        'FieldName': finding.get('field_name'),
                        'FilePath': finding.get('file_path'),
                        'FileName': finding.get('file_name'),
                        'FileExtension': finding.get('file_extension'),
                        'FindingCount': finding.get('finding_count', 1),
                        'AverageConfidence': finding.get('average_confidence', 0.0),
                        'MaxConfidence': finding.get('max_confidence', 0.0),
                        'SampleFindings': finding.get('sample_findings'),
                        'TotalRowsInSource': finding.get('total_rows_in_source'),
                        'NonNullRowsScanned': finding.get('non_null_rows_scanned')
                    }
                    summaries.append(summary)
                
                # Bulk insert summaries
                await session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(ScanFindingSummary, summaries))
                await session.commit()
                
                self.logger.log_database_operation("BULK INSERT", "ScanFindingSummaries", "SUCCESS", 
                                                 record_count=len(summaries), **context)
                return len(summaries)
                
        except Exception as e:
            self.error_handler.handle_error(e, context="insert_scan_findings", 
                                          finding_count=len(findings), **context)
            raise

    async def get_objects_for_classification(self, object_ids: List[str], context: Optional[Dict[str, Any]] = None) -> List[OrmDiscoveredObject]:
        """Get discovered objects by their IDs for classification."""
        context = context or {}
        if not object_ids:
            return []
            
        self.logger.log_database_operation("SELECT", "DiscoveredObjects", "STARTED", 
                                         operation="get_for_classification", 
                                         object_count=len(object_ids), **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(OrmDiscoveredObject).where(OrmDiscoveredObject.ID.in_(object_ids))
                result = await session.scalars(stmt)
                results = list(result.all())
                
                self.logger.log_database_operation("SELECT", "DiscoveredObjects", "SUCCESS", 
                                                 operation="get_for_classification", 
                                                 row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_objects_for_classification", 
                                          object_count=len(object_ids), **context)
            raise

    async def update_classification_timestamps(self, object_ids: List[int], context: Optional[Dict[str, Any]] = None) -> None:
        """Update classification timestamps for objects."""
        context = context or {}
        if not object_ids:
            return
            
        self.logger.log_database_operation("UPDATE", "DiscoveredObjectClassificationDateInfo", "STARTED", 
                                         operation="update_timestamps", 
                                         object_count=len(object_ids), **context)
        try:
            async with self.get_async_session() as session:
                current_time = datetime.now(timezone.utc)
                
                # Update existing records
                stmt = update(DiscoveredObjectClassificationDateInfo).where(
                    DiscoveredObjectClassificationDateInfo.ObjectID.in_(object_ids)
                ).values(LastClassificationDate=current_time)
                
                result = await session.execute(stmt)
                updated_count = result.rowcount
                
                # Insert records for objects that don't have classification info yet
                existing_result = await session.scalars(
                    select(DiscoveredObjectClassificationDateInfo.ObjectID)
                    .where(DiscoveredObjectClassificationDateInfo.ObjectID.in_(object_ids))
                )
                existing_ids = list(existing_result.all())
                
                new_ids = set(object_ids) - set(existing_ids)
                if new_ids:
                    new_records = [
                        {
                            'ObjectID': obj_id,
                            'LastClassificationDate': current_time
                        }
                        for obj_id in new_ids
                    ]
                    await session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(DiscoveredObjectClassificationDateInfo, new_records))
                
                await session.commit()
                
                self.logger.log_database_operation("UPDATE", "DiscoveredObjectClassificationDateInfo", "SUCCESS", 
                                                 operation="update_timestamps", 
                                                 updated_count=updated_count,
                                                 inserted_count=len(new_ids) if new_ids else 0, **context)
                
        except Exception as e:
            self.error_handler.handle_error(e, context="update_classification_timestamps", 
                                          object_count=len(object_ids), **context)
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
                    .where(OrmDiscoveredObject.DataSourceID == datasource_id)
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
        """Insert batch of discovered objects into staging table."""
        context = context or {}
        if not objects:
            return 0
            
        self._validate_table_name(staging_table_name)
        self.logger.log_database_operation("BULK INSERT", staging_table_name, "STARTED", 
                                         record_count=len(objects), **context)
        try:
            async with self.get_async_session() as session:
                # Convert objects to proper format with hash calculation
                processed_objects = []
                for obj in objects:
                    # Calculate object key hash
                    import hashlib
                    key_components = [
                        obj.get('DataSourceID', ''),
                        obj.get('ObjectPath', ''),
                        obj.get('ObjectType', '')
                    ]
                    key_string = '|'.join(str(comp) for comp in key_components)
                    object_key_hash = hashlib.sha256(key_string.encode()).digest()
                    
                    processed_obj = {
                        'ObjectKeyHash': object_key_hash,
                        'DataSourceID': obj.get('DataSourceID'),
                        'ObjectType': obj.get('ObjectType'),
                        'ObjectPath': obj.get('ObjectPath'),
                        'SizeBytes': obj.get('SizeBytes'),
                        'CreatedDate': obj.get('CreatedDate'),
                        'LastModified': obj.get('LastModified'),
                        'LastAccessed': obj.get('LastAccessed'),
                        'DiscoveryTimestamp': datetime.now(timezone.utc)
                    }
                    processed_objects.append(processed_obj)
                
                # Insert into staging table using raw SQL for performance
                def sync_insert(sync_session):
                    staging_table = Table(staging_table_name, Base.metadata, autoload_with=sync_session.bind)
                    sync_session.execute(staging_table.insert(), processed_objects)
                
                await session.run_sync(sync_insert)
                await session.commit()
                
                self.logger.log_database_operation("BULK INSERT", staging_table_name, "SUCCESS", 
                                                 record_count=len(processed_objects), **context)
                return len(processed_objects)
                
        except Exception as e:
            self.error_handler.handle_error(e, context="insert_discovered_object_batch", 
                                          staging_table=staging_table_name,
                                          object_count=len(objects), **context)
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

    async def test_database_connection(self, context: Optional[Dict[str, Any]] = None) -> bool:
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
                stmt = select(Task.Status, func.count(Task.Status)).where(Task.JobID == job_id).group_by(Task.Status)
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
    
    async def create_job_execution(self, template_id: str, execution_id: str, trigger_type: str, context: Optional[Dict[str, Any]] = None) -> Job:
        context = context or {}
        self.logger.log_database_operation("INSERT", "Jobs", "STARTED", execution_id=execution_id, **context)
        try:
            async with self.get_async_session() as session:
                template_stmt = select(ScanTemplate).where(ScanTemplate.template_id == template_id)
                template_result = await session.scalars(template_stmt)
                scan_template = template_result.one()
                new_job = Job(execution_id=execution_id, scan_template_id=scan_template.id, status="QUEUED", trigger_type=trigger_type)
                session.add(new_job)
                await session.commit()
                await session.refresh(new_job)
                self.logger.log_database_operation("INSERT", "Jobs", "SUCCESS", job_id=new_job.id, **context)
                return new_job
        except Exception as e:
            self.error_handler.handle_error(e, context="create_job_execution", **context)
            raise

    async def create_task(self, job_id: int, task_type: str, work_packet: Dict[str, Any], datasource_id: Optional[str] = None, parent_task_id: Optional[int] = None, context: Optional[Dict[str, Any]] = None) -> Task:
        context = context or {}
        self.logger.log_database_operation("INSERT", "Tasks", "STARTED", job_id=job_id, task_type=task_type, **context)
        try:
            async with self.get_async_session() as session:
                new_task = Task(JobID=job_id, TaskType=task_type, WorkPacket=work_packet, DatasourceID=datasource_id, ParentTaskID=parent_task_id, Status=TaskStatus.PENDING)
                session.add(new_task)
                await session.commit()
                await session.refresh(new_task)
                self.logger.log_database_operation("INSERT", "Tasks", "SUCCESS", task_id=new_task.ID, **context)
                return new_task
        except Exception as e:
            self.error_handler.handle_error(e, context="create_task", **context)
            raise

    async def assign_task_to_worker(self, task_id: int, worker_id: str, lease_duration_seconds: int, context: Optional[Dict[str, Any]] = None) -> bool:
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Tasks", "STARTED", task_id=task_id, worker_id=worker_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Task).where(Task.ID == task_id, Task.Status == TaskStatus.PENDING).values(
                    Status=TaskStatus.ASSIGNED,
                    WorkerID=worker_id,
                    LeaseExpiry=datetime.now(timezone.utc) + timedelta(seconds=lease_duration_seconds),
                    RetryCount=Task.RetryCount + 1
                )
                result = await session.execute(stmt)
                await session.commit()
                was_successful = result.rowcount > 0
                status = "SUCCESS" if was_successful else "FAILURE_RACE_CONDITION"
                self.logger.log_database_operation("UPDATE", "Tasks", status, task_id=task_id, **context)
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(e, context="assign_task_to_worker", **context)
            raise

    async def complete_task(self, task_id: int, context: Optional[Dict[str, Any]] = None) -> None:
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Tasks", "STARTED", operation="complete_task", task_id=task_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Task).where(Task.ID == task_id).values(Status=TaskStatus.COMPLETED)
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "Tasks", "SUCCESS", operation="complete_task", task_id=task_id, **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="complete_task", task_id=task_id, **context)
            raise

    async def fail_task(self, task_id: int, is_retryable: bool, max_retries: int = 3, context: Optional[Dict[str, Any]] = None) -> None:
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Tasks", "STARTED", operation="fail_task", task_id=task_id, **context)
        try:
            async with self.get_async_session() as session:
                task = await session.get(Task, task_id)
                if not task: return
                
                if is_retryable and task.RetryCount < max_retries:
                    task.Status = TaskStatus.PENDING
                    task.WorkerID = None
                    task.LeaseExpiry = None
                else:
                    task.Status = TaskStatus.FAILED
                await session.commit()
                self.logger.log_database_operation("UPDATE", "Tasks", "SUCCESS", operation="fail_task", new_status=task.Status.value, **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="fail_task", task_id=task_id, **context)
            raise

    async def get_pending_tasks_batch(self, job_id: int, batch_size: int, context: Optional[Dict[str, Any]] = None) -> List[Task]:
        context = context or {}
        self.logger.log_database_operation("SELECT", "Tasks", "STARTED", job_id=job_id, batch_size=batch_size, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(Task).where(Task.JobID == job_id, Task.Status == TaskStatus.PENDING).order_by(Task.ID).limit(batch_size)
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
                stmt = select(Task).where(Task.Status == TaskStatus.ASSIGNED, Task.LeaseExpiry < expiry_threshold)
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

    async def write_task_output_record(self, task_id: int, output_type: str, payload: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> TaskOutputRecord:
        context = context or {}
        self.logger.log_database_operation("INSERT", "TaskOutputRecords", "STARTED", task_id=task_id, **context)
        try:
            async with self.get_async_session() as session:
                record = TaskOutputRecord(TaskID=task_id, OutputType=output_type, OutputPayload=payload, Status='PENDING_PROCESSING')
                session.add(record)
                await session.commit()
                await session.refresh(record)
                self.logger.log_database_operation("INSERT", "TaskOutputRecords", "SUCCESS", record_id=record.ID, **context)
                return record
        except Exception as e:
            self.error_handler.handle_error(e, context="write_task_output_record", **context)
            raise

    async def get_pending_output_records(self, limit: int = 100, context: Optional[Dict[str, Any]] = None) -> List[TaskOutputRecord]:
        context = context or {}
        self.logger.log_database_operation("SELECT", "TaskOutputRecords", "STARTED", limit=limit, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(TaskOutputRecord).where(TaskOutputRecord.Status == 'PENDING_PROCESSING').limit(limit)
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
                # Explicitly define columns for the staging table to avoid initialization errors
                staging_table = Table(
                    staging_table_name,
                    Base.metadata,
                    Column('ID', Integer, primary_key=True, autoincrement=True),
                    Column('ObjectKeyHash', LargeBinary(32), nullable=False),
                    Column('DataSourceID', String(255), nullable=False),
                    Column('ObjectType', String(50), nullable=False),
                    Column('ObjectPath', String(4000), nullable=False),
                    Column('SizeBytes', Integer, nullable=True),
                    Column('CreatedDate', DateTime, nullable=True),
                    Column('LastModified', DateTime, nullable=True),
                    Column('LastAccessed', DateTime, nullable=True),
                    Column('DiscoveryTimestamp', DateTime(timezone=True), nullable=False),
                    extend_existing=True # Keep this for validation script robustness
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
        context = context or {}
        self._validate_table_name(staging_table_name)
        self.logger.log_database_operation("DELTA", staging_table_name, "STARTED", **context)
        try:
            async with self.get_async_session() as session:
                staging_hashes_query = text(f'SELECT "ObjectKeyHash" FROM {staging_table_name}')
                staging_result = await session.execute(staging_hashes_query)
                staging_hashes = {val.hex() for val in staging_result.scalars().all()}
                
                main_hashes_query = select(OrmDiscoveredObject.ObjectKeyHash).where(OrmDiscoveredObject.DataSourceID == datasource_id)
                main_result = await session.execute(main_hashes_query)
                main_hashes = {val.hex() for val in main_result.scalars().all()}

                new_hashes_hex = staging_hashes - main_hashes
                deleted_hashes_hex = main_hashes - staging_hashes
                
                if deleted_hashes_hex:
                    deleted_hashes_bytes = [bytes.fromhex(h) for h in deleted_hashes_hex]
                    delete_stmt = delete(OrmDiscoveredObject).where(OrmDiscoveredObject.ObjectKeyHash.in_(deleted_hashes_bytes))
                    await session.execute(delete_stmt)
                
                if new_hashes_hex:
                    new_hashes_bytes = [bytes.fromhex(h) for h in new_hashes_hex]
                    target_columns = [c.name for c in OrmDiscoveredObject.__table__.columns if c.name != 'ID']
                    target_columns_str = ", ".join([f'"{c}"' for c in target_columns])
                    new_records_sql = text(f'SELECT {target_columns_str} FROM {staging_table_name} WHERE "ObjectKeyHash" IN :hashes')
                    new_records_result = await session.execute(new_records_sql, {"hashes": tuple(new_hashes_bytes)})
                    new_records_mappings = new_records_result.mappings().all()
                    if new_records_mappings:
                        await session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(OrmDiscoveredObject, new_records_mappings))

                await session.commit()
                
                results = {"new": len(new_hashes_hex), "modified": 0, "deleted": len(deleted_hashes_hex)}
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
                stmt = select(DiscoveredObjectClassificationDateInfo.ObjectID).where(DiscoveredObjectClassificationDateInfo.LastClassificationDate < cutoff_date)
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
                result = result.one_or_none()
                
                self.logger.log_database_operation("SELECT", "DataSources", "SUCCESS", operation="get_with_schedule", found=(result is not None), **context)
                return result
        except Exception as e:
            self.error_handler.handle_error(e, context="get_datasource_with_schedule", datasource_id=datasource_id, **context)
            raise

    async def get_active_jobs(self, statuses: List[JobStatus] = None, context: Optional[Dict[str, Any]] = None) -> List[Job]:
        """Fetches jobs that are in an active state (defaulting to QUEUED or RUNNING)."""
        context = context or {}
        if statuses is None:
            statuses = [JobStatus.QUEUED, JobStatus.RUNNING]
        
        self.logger.log_database_operation("SELECT", "Jobs", "STARTED", operation="get_active_jobs", statuses=statuses, **context)
        try:
            async with self.get_async_session() as session:
                stmt = select(Job).where(Job.status.in_(statuses))
                result = await session.scalars(stmt)
                results = list(result.all())
                self.logger.log_database_operation("SELECT", "Jobs", "SUCCESS", operation="get_active_jobs", row_count=len(results), **context)
                return results
        except Exception as e:
            self.error_handler.handle_error(e, context="get_active_jobs", **context)
            raise

    async def get_task_by_id(self, task_id: int, context: Optional[Dict[str, Any]] = None) -> Optional[Task]:
        """Fetches a single task by its primary key."""
        context = context or {}
        self.logger.log_database_operation("SELECT", "Tasks", "STARTED", operation="get_by_id", task_id=task_id, **context)
        try:
            async with self.get_async_session() as session:
                result = await session.get(Task, task_id)
                self.logger.log_database_operation("SELECT", "Tasks", "SUCCESS", operation="get_by_id", found=(result is not None), **context)
                return result
        except Exception as e:
            self.error_handler.handle_error(e, context="get_task_by_id", task_id=task_id, **context)
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

    async def update_output_record_status(self, record_id: int, status: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Updates the status of a specific TaskOutputRecord."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "TaskOutputRecords", "STARTED", operation="update_status", record_id=record_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(TaskOutputRecord).where(TaskOutputRecord.ID == record_id).values(Status=status)
                await session.execute(stmt)
                await session.commit()
                self.logger.log_database_operation("UPDATE", "TaskOutputRecords", "SUCCESS", operation="update_status", **context)
        except Exception as e:
            self.error_handler.handle_error(e, context="update_output_record_status", record_id=record_id, **context)
            raise

    async def extend_task_lease(self, task_id: int, duration_seconds: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """Extends the lease of an ASSIGNED task, preventing it from timing out."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Tasks", "STARTED", operation="extend_lease", task_id=task_id, **context)
        try:
            async with self.get_async_session() as session:
                new_expiry = datetime.now(timezone.utc) + timedelta(seconds=duration_seconds)
                # The WHERE clause ensures we only update an active, assigned task (optimistic locking).
                stmt = update(Task).where(
                    Task.ID == task_id,
                    Task.Status == TaskStatus.ASSIGNED
                ).values(LeaseExpiry=new_expiry)
                result = await session.execute(stmt)
                await session.commit()
                was_successful = result.rowcount > 0
                self.logger.log_database_operation("UPDATE", "Tasks", "SUCCESS" if was_successful else "FAILURE", operation="extend_lease", **context)
                return was_successful
        except Exception as e:
            self.error_handler.handle_error(e, context="extend_task_lease", task_id=task_id, **context)
            raise

    async def cancel_pending_tasks_for_job(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> int:
        """
        Cancels all PENDING tasks for a job.
        NOTE: This method strictly handles the database state change. It is the
        Orchestrator's responsibility to handle resource cleanup with the
        ResourceCoordinator for any affected tasks.
        """
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Tasks", "STARTED", operation="cancel_pending", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                stmt = update(Task).where(
                    Task.JobID == job_id,
                    Task.Status == TaskStatus.PENDING
                ).values(Status=TaskStatus.CANCELLED)
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
        try:
            async with self.get_async_session() as session:
                stmt = select(ScanTemplate).where(ScanTemplate.template_id == template_id)
                result = await session.scalars(stmt)
                result = result.one_or_none()
                self.logger.log_database_operation("SELECT", "ScanTemplates", "SUCCESS", found=(result is not None), **context)
                return result
        except Exception as e:
            self.error_handler.handle_error(e, context="get_scan_template_by_id", **context)
            raise

    async def transition_job_from_pausing_to_paused(self, job_id: int, context: Optional[Dict[str, Any]] = None) -> bool:
        """Atomically transitions a job from PAUSING to PAUSED only if no tasks are assigned."""
        context = context or {}
        self.logger.log_database_operation("UPDATE", "Jobs", "STARTED", operation="atomic_pause_transition", job_id=job_id, **context)
        try:
            async with self.get_async_session() as session:
                # Subquery to check for active tasks
                active_task_subquery = select(Task.ID).where(Task.JobID == job_id, Task.Status == TaskStatus.ASSIGNED).limit(1).exists()
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