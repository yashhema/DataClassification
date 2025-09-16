# src/core/config/config_models.py
"""
Defines strongly-typed Pydantic models for complex JSON configurations
stored in the database. These models provide validation and a single
source of truth for configuration structures.
"""

from typing import List, Optional, Dict, Any, Union, Literal, Annotated
from pydantic import BaseModel, Field

# =================================================================
# Section 1: Universal Connection & Authentication Models
# =================================================================

# --- 1.1: Database Connection Models ---
# --- Define a model for each specific authentication method ---

class WindowsAuthConfig(BaseModel):
    auth_method: Literal["windows"]

class StandardAuthConfig(BaseModel):
    auth_method: Literal["standard"]
    username: str = Field(..., description="Username for standard password authentication.")

class ADAuthConfig(BaseModel):
    auth_method: Literal["ad"]
    username: str = Field(..., description="The AD username.")
    domain: Optional[str] = Field(None, description="The AD domain.")

class CertificateAuthConfig(BaseModel):
    auth_method: Literal["certificate"]
    client_cert_path: str = Field(..., description="Path to the client's SSL certificate.")
    client_key_path: str = Field(..., description="Path to the client's SSL private key.")

# --- Create a Discriminated Union of all auth types ---
DbAuthConfig = Annotated[
    Union[WindowsAuthConfig, StandardAuthConfig, ADAuthConfig, CertificateAuthConfig],
    Field(discriminator="auth_method")
]

class DbAuthConfig(BaseModel):
    username: Optional[str] = Field(None, description="Username for standard password authentication.")
    integrated_security: bool = Field(False, description="Set to true for Windows Authentication (SQL Server).")
    kerberos_principal: Optional[str] = Field(None, description="The full Kerberos principal name, e.g., 'user@REALM'.")
    iam_user_or_role: Optional[str] = Field(None, description="IAM user/role for generating a temporary database token (e.g., for AWS RDS).")
    aws_region: Optional[str] = Field(None, description="The AWS region for RDS IAM authentication.")

class TlsConfig(BaseModel):
    encrypt_connection: bool = Field(True, description="Master toggle for encrypted connections.")
    ssl_mode: Optional[str] = Field("verify-ca", description="SSL verification mode (e.g., 'require', 'verify-ca', 'verify-full').")
    client_cert_path: Optional[str] = Field(None, description="Path on the worker to the client's SSL certificate.")
    client_key_path: Optional[str] = Field(None, description="Path on the worker to the client's SSL private key.")
    server_ca_path: Optional[str] = Field(None, description="Path on the worker to the CA certificate for server verification.")
    trust_server_certificate: bool = Field(False, description="Bypasses server certificate validation. Not for production.")

class DatabaseConnectionConfig(BaseModel):
    host: str = Field(..., description="Hostname or IP address of the database server.")
    port: int = Field(..., description="Database port number.")
    database: Optional[str] = Field(None, description="Database name (for SQL Server, PostgreSQL, MySQL).")
    oracle_service_name: Optional[str] = Field(None, description="Oracle Service Name.")
    oracle_sid: Optional[str] = Field(None, description="Oracle System Identifier (SID).")
    driver: Optional[str] = Field(None, description="ODBC or JDBC driver name.")
    auth: DbAuthConfig = Field(default_factory=DbAuthConfig)
    tls: TlsConfig = Field(default_factory=TlsConfig)
    connection_timeout_seconds: int = 30
    command_timeout_seconds: int = 300
    auth: DbAuthConfig

# --- 1.2: File & Object Store Connection Models (NEW) ---

class StandardFileAuth(BaseModel):
    auth_method: Literal["standard"]
    username: Optional[str] = Field(None, description="Username for SMB, NFS, SharePoint, etc.")
    # Password is retrieved from the secure vault via credential_id

class AwsAuth(BaseModel):
    auth_method: Literal["aws"]
    # Access/Secret keys are retrieved via credential_id
    iam_role_arn: Optional[str] = Field(None, description="IAM Role ARN for the worker to assume.")
    aws_region: str = Field(..., description="The AWS region of the S3 bucket.")

class AzureAuth(BaseModel):
    auth_method: Literal["azure"]
    # Connection strings or SP secrets are retrieved via credential_id
    storage_account_name: str = Field(..., description="The name of the Azure Storage Account.")
    service_principal_id: Optional[str] = Field(None, description="The Client ID of the App Registration.")
    
class GcpAuth(BaseModel):
    auth_method: Literal["gcp"]
    # Service account key JSON is retrieved via credential_id
    project_id: str = Field(..., description="The GCP Project ID.")

FileAuthConfig = Annotated[Union[StandardFileAuth, AwsAuth, AzureAuth, GcpAuth], Field(discriminator="auth_method")]

class FileConnectionConfig(BaseModel):
    """Universal model for file and object store connection parameters."""
    connection_type: Literal["smb", "nfs", "s3", "azure_blob", "gcs", "sharepoint", "dropbox"]
    endpoint_url: Optional[str] = Field(None, description="The full URL for the connection (e.g., for SharePoint, or a custom S3-compatible endpoint).")
    host: Optional[str] = Field(None, description="The hostname or IP for on-premise systems like SMB or NFS.")
    share_or_bucket: Optional[str] = Field(None, description="The name of the SMB share, NFS export, or cloud bucket/container.")
    auth: FileAuthConfig

# =================================================================
# Section 2: Scan Profile Models
# =================================================================

class BaseProfile(BaseModel):
    """A base model to ensure all profiles are enabled and have a credential."""
    profile_type: str
    enabled: bool = True
    credential_id: str = Field(..., description="The ID of the credential to use for this specific scan profile.")

class DataClassificationProfile(BaseProfile):
    """Defines a scan for discovering and classifying sensitive data."""
    profile_type: Literal["data_classification"]
    discovery_rules: "DiscoveryRules" = Field(default_factory=lambda: DiscoveryRules())
    scan_config: "ScanConfig" = Field(default_factory=lambda: ScanConfig())
    failure_thresholds: "FailureThresholds" = Field(default_factory=lambda: FailureThresholds())

class EntitlementProfile(BaseProfile):
    """Defines a scan for analyzing user permissions and entitlements."""
    profile_type: Literal["entitlement"]
    # A list of query names from the ConnectorConfiguration to execute
    entitlement_query_names: List[str] = ["server_roles", "object_permissions"]
    target_users: List[str] = Field(default_factory=list, description="If not empty, limits scan to these users.")
    target_roles: List[str] = Field(default_factory=list, description="If not empty, limits scan to these roles.")

class BenchmarkProfile(BaseProfile):
    """Defines a scan for checking system configuration against a security benchmark."""
    profile_type: Literal["benchmark"]
    benchmark_name: str = Field(..., description="The name of the benchmark to use (e.g., 'CIS_SQL_SERVER_v1.5').")
    # The benchmark queries will be stored in the ConnectorConfiguration

# A union of all possible profile types
ScanProfile = Annotated[Union[DataClassificationProfile, EntitlementProfile, BenchmarkProfile], Field(discriminator="profile_type")]

# --- Helper models for the DataClassificationProfile ---

class DatabaseExclusions(BaseModel):
    static_exclusions: List[str] = Field(default_factory=list)
    pattern_exclusions: List[str] = Field(default_factory=list)
    system_exclusions: List[str] = ["master", "model", "msdb", "tempdb", "postgres"]
    custom_exclusions: List[str] = Field(default_factory=list)

class SchemaExclusions(BaseModel):
    system_schemas: List[str] = ["sys", "INFORMATION_SCHEMA", "guest", "pg_catalog"]
    pattern_exclusions: List[str] = Field(default_factory=list)
    custom_exclusions: List[str] = Field(default_factory=list)

class DiscoveryLimits(BaseModel):
    max_databases_per_instance: int = 100
    max_tables_per_database: int = 1000
    min_table_rows: int = 10
    include_views: bool = False
    include_empty_tables: bool = False

class DiscoveryRules(BaseModel):
    environment_type: str = "on_premise"
    database_exclusions: DatabaseExclusions = Field(default_factory=DatabaseExclusions)
    schema_exclusions: SchemaExclusions = Field(default_factory=SchemaExclusions)
    discovery_limits: DiscoveryLimits = Field(default_factory=DiscoveryLimits)

class ConnectionLimits(BaseModel):
    max_connections: int = 1
    connection_pool_size: int = 2

class ScanConfig(BaseModel):
    include_schemas: List[str] = Field(default_factory=list)
    exclude_tables: List[str] = Field(default_factory=list)
    max_sample_rows: int = 1000
    connection_limits: ConnectionLimits = Field(default_factory=ConnectionLimits)

class FailureThresholds(BaseModel):
    access_denied: Optional[int] = 100
    corrupted_file: Optional[int] = 500
    unsupported_format: Optional[int] = 10000

# =================================================================
# Section 3: The New Root Model for DataSource.configuration
# =================================================================

class DataSourceConfiguration(BaseModel):
    """
    The new root model for the DataSource.configuration JSON field.
    It contains the connection details and a list of scan profiles to run.
    """
    connection: Union[DatabaseConnectionConfig, FileConnectionConfig]
    scan_profiles: List[ScanProfile]

# =================================================================
# Section 4: Models for ConnectorConfiguration.Configuration
# =================================================================

class BaseConnectorConfig(BaseModel):
    connector_family: str

class EnvironmentQueries(BaseModel):
    database_discovery: str
    schema_discovery: str
    table_discovery: str
    row_sampling: str
    row_sampling_with_tablesample: Optional[str] = None

class CommonQueries(BaseModel):
    primary_key_discovery: str
    column_metadata: str
    table_row_count: str
    table_size_info: str

class FallbackQueries(BaseModel):
    database_discovery: str
    schema_discovery: str
    table_discovery: str
    row_sampling: str
    column_metadata: str

class DatabaseQuerySet(BaseModel):
    environments: Dict[str, EnvironmentQueries]
    common_queries: CommonQueries
    fallback_queries: FallbackQueries
    # NEW: Dictionaries to hold queries for new scan types
    entitlement_queries: Dict[str, str] = Field(default_factory=dict, description="e.g., {'server_roles': 'SELECT ...', 'object_permissions': 'SELECT ...'}")
    benchmark_queries: Dict[str, Dict[str, str]] = Field(default_factory=dict, description="e.g., {'CIS_SQL_SERVER_v1.5': {'check_1.1': 'SELECT ...'}}")

class QuerySettings(BaseModel):
    default_timeout_seconds: int = 300
    max_retries: int = 3

class DatabaseConnectorConfig(BaseConnectorConfig):
    connector_family: Literal["database"]
    query_sets: Dict[str, DatabaseQuerySet]
    query_settings: QuerySettings

class FileConnectorConfig(BaseConnectorConfig):
    connector_family: Literal["file"]
    default_text_encoding: str = "utf-8"

class ConnectorConfiguration(BaseModel):
    config: Annotated[
        Union[DatabaseConnectorConfig, FileConnectorConfig],
        Field(discriminator="connector_family")
    ]

