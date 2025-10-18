# src/core/db_models/__init__.py
"""
SQLAlchemy ORM Models Package

CRITICAL: Import order matters for relationship resolution.
Models must be imported in dependency order to prevent "failed to locate a name" errors.

Dependency Analysis:
===================

LEVEL 0 - Foundation:
  - base.py: DeclarativeBase (no dependencies)

LEVEL 1 - Association Tables & Independent Models:
  - association_tables.py: Many-to-many link tables (depends on Base only)
  - calendar_schema.py: Calendar, CalendarRule (internal relationships only)
  - dictionary_schema.py: Dictionary (forward ref to Classifier via TYPE_CHECKING)
  - vulnerability_schema.py: CVE tables (internal relationships only)
  - entitlement_schema.py: Entitlement tables (internal relationships only)
  - discovery_catalog_schema.py: DiscoveredObject, ObjectMetadata (no FK relationships)
  - findings_schema.py: ScanFindingSummary, ScanFindingOccurrence (no FK relationships)
  - error_log_schema.py: SystemErrorLog (no relationships)
  - remediation_ledger_schema.py: RemediationLedger (no relationships)
  - connector_config_schema.py: ConnectorConfiguration (no relationships)
  - credentials_schema.py: Credential (no relationships)
  - processing_status_schema.py: ObjectProcessingStatus (no relationships)

LEVEL 2 - Core Domain Models:
  - datasource_schema.py: 
      * NodeGroup (no dependencies)
      * Tag (self-referential)
      * DataSource (uses association_tables, forward refs to Calendar & OverrideGroup)
      * DataSourceMetadata (references DataSource)
  - classifier_schema.py:
      * Category (no dependencies)
      * Classifier (uses association_tables, dictionary_schema, forward ref to ClassifierTemplate)
      * ClassifierPattern, ClassifierContextRule, etc. (reference Classifier)

LEVEL 3 - Templates & Complex Models:
  - classifiertemplate_schema.py: ClassifierTemplate (uses association_tables, references Classifier)
  - compliance_schemas.py: 
      * QuerySet, Query (internal relationships)
      * OverrideGroup (uses association_tables, forward ref to DataSource)
      * OverrideRule, BenchmarkFinding, etc.
  - system_parameters_schema.py: SystemParameter (references NodeGroup)
  - job_schema.py:
      * MasterJob, MasterJobStateSummary (no external dependencies)
      * PolicyTemplate, ScanTemplate (reference Job with forward refs)
      * Job (references templates with forward refs)
      * Task (references Job)
      * TaskOutputRecord (references Task, no FK constraint)

LEVEL 4 - Dependent Models:
  - enumeration_progress_schema.py: EnumerationProgress (references Task from job_schema)
"""

# ==============================================================================
# LEVEL 0: Foundation
# ==============================================================================
from .base import Base

# ==============================================================================
# LEVEL 1: Association Tables & Independent Models
# ==============================================================================

# Association tables MUST come before models that use them
from .association_tables import (
    ClassifierTemplateLink,
    DataSourceTagLink,
    DatasourceToOverrideGroupLink
)

# Independent models with no external relationships or only forward references
from .calendar_schema import (
    Calendar,
    CalendarRule,
    DayOfWeek
)

from .dictionary_schema import (
    Dictionary
)

from .vulnerability_schema import (
    VulnerabilityFinding,
    CVEDefinition,
    ProductCatalog,
    CVEAffectedRange,
    CVEMitigationOption
)

from .entitlement_schema import (
    EntitlementSnapshot,
    EntitlementPrincipal,
    EntitlementMembership,
    EntitlementPermission,
    RawEntitlementData
)

from .discovery_catalog_schema import (
    DiscoveredObject,
    ObjectMetadata,
    DiscoveredObjectClassificationDateInfo
)

from .findings_schema import (
    ScanFindingSummary,
    ScanFindingOccurrence
)

from .error_log_schema import (
    SystemErrorLog
)

from .remediation_ledger_schema import (
    RemediationLedger,
    LedgerStatus
)

from .connector_config_schema import (
    ConnectorConfiguration
)

from .credentials_schema import (
    Credential
)

from .processing_status_schema import (
    ObjectProcessingStatus
)

# ==============================================================================
# LEVEL 2: Core Domain Models
# ==============================================================================

# DataSource models - NodeGroup must be available before Classifier models
from .datasource_schema import (
    NodeGroup,
    Tag,
    DataSource,
    DataSourceMetadata
)

# Classifier models - Dictionary is already imported, ClassifierTemplate uses forward ref
from .classifier_schema import (
    Category,
    Classifier,
    ClassifierPattern,
    RuleType,
    ClassifierContextRule,
    ClassifierValidationRule,
    ClassifierExcludeTerm
)

# ==============================================================================
# LEVEL 3: Templates & Complex Models
# ==============================================================================

# ClassifierTemplate - Classifier is now available
from .classifiertemplate_schema import (
    ClassifierTemplate
)

# Compliance schemas - DataSource is now available (forward ref resolved)
from .compliance_schemas import (
    QuerySet,
    Query,
    OverrideGroup,
    OverrideRule,
    BenchmarkFinding,
    SensitiveDataLocation,
    ScanCycle
)

# SystemParameter - NodeGroup is now available
from .system_parameters_schema import (
    SystemParameter,
    ComponentType
)

# Job models - All templates are now available
from .job_schema import (
    JobType,
    JobStatus,
    TaskStatus,
    MasterJob,
    MasterJobStateSummary,
    PolicyTemplate,
    ScanTemplate,
    Job,
    Task,
    TaskOutputRecord
)

# ==============================================================================
# LEVEL 4: Dependent Models
# ==============================================================================

# EnumerationProgress - Task is now available
from .enumeration_progress_schema import (
    EnumerationProgress,
    EnumerationProgressStatus
)

# ==============================================================================
# Public API - All models available for import
# ==============================================================================

__all__ = [
    # Base
    "Base",
    
    # Association Tables
    "ClassifierTemplateLink",
    "DataSourceTagLink",
    "DatasourceToOverrideGroupLink",
    
    # Calendar
    "Calendar",
    "CalendarRule",
    "DayOfWeek",
    
    # Dictionary
    "Dictionary",
    
    # Vulnerability
    "VulnerabilityFinding",
    "CVEDefinition",
    "ProductCatalog",
    "CVEAffectedRange",
    "CVEMitigationOption",
    
    # Entitlement
    "EntitlementSnapshot",
    "EntitlementPrincipal",
    "EntitlementMembership",
    "EntitlementPermission",
    "RawEntitlementData",
    
    # Discovery Catalog
    "DiscoveredObject",
    "ObjectMetadata",
    "DiscoveredObjectClassificationDateInfo",
    
    # Findings
    "ScanFindingSummary",
    "ScanFindingOccurrence",
    
    # Error Log
    "SystemErrorLog",
    
    # Remediation
    "RemediationLedger",
    "LedgerStatus",
    
    # Connector Config
    "ConnectorConfiguration",
    
    # Credentials
    "Credential",
    
    # Processing Status
    "ObjectProcessingStatus",
    
    # DataSource
    "NodeGroup",
    "Tag",
    "DataSource",
    "DataSourceMetadata",
    
    # Classifier
    "Category",
    "Classifier",
    "ClassifierPattern",
    "RuleType",
    "ClassifierContextRule",
    "ClassifierValidationRule",
    "ClassifierExcludeTerm",
    
    # Classifier Template
    "ClassifierTemplate",
    
    # Compliance
    "QuerySet",
    "Query",
    "OverrideGroup",
    "OverrideRule",
    "BenchmarkFinding",
    "SensitiveDataLocation",
    "ScanCycle",
    
    # System Parameters
    "SystemParameter",
    "ComponentType",
    
    # Job
    "JobType",
    "JobStatus",
    "TaskStatus",
    "MasterJob",
    "MasterJobStateSummary",
    "PolicyTemplate",
    "ScanTemplate",
    "Job",
    "Task",
    "TaskOutputRecord",
    
    # Enumeration Progress
    "EnumerationProgress",
    "EnumerationProgressStatus",
]