"""PolicyWorker configuration models"""

from pydantic import BaseModel, Field
from typing import Literal, Optional

class DatabaseConfig(BaseModel):
    """Database connection configuration"""
    url: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_pre_ping: bool = True

class S3Config(BaseModel):
    """S3 client configuration"""
    bucket_name: str
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # For LocalStack/MinIO

class AthenaConfig(BaseModel):
    """Athena client configuration"""
    database: str
    output_bucket: str
    workgroup: str = "primary"
    region: str = "us-east-1"

class FlinkConfig(BaseModel):
    """Flink API configuration for merge job submission"""
    jobmanager_url: str  # e.g., http://flink-jobmanager:8081
    jar_path: str  # S3 path to merge job JAR
    parallelism: int = 32
    taskmanager_memory: str = "8g"

class PolicyWorkerConfig(BaseModel):
    """Root configuration for PolicyWorker"""
    worker_id: str
    component_name: str = "PolicyWorker"
    deployment_mode: Literal["standalone", "single_process"]
    node_group: str = "central"

    # Backend configuration
    backend_url: Optional[str] = None  # For standalone mode

    # Client configs
    db_config: DatabaseConfig
    s3_config: S3Config
    athena_config: AthenaConfig
    flink_config: FlinkConfig

    # Operational settings
    task_poll_interval_seconds: int = 5
    heartbeat_interval_seconds: int = 30
    max_retry_attempts: int = 3

    # Logging
    log_format: Literal["TEXT", "JSON"] = "JSON"
    log_level: str = "INFO"

    # Metrics
    enable_metrics: bool = True
    metrics_port: int = 9090

    @classmethod
    def from_env(cls):
        """Load configuration from environment variables"""
        import os
        return cls(
            worker_id=os.getenv("WORKER_ID", "policy-worker-1"),
            deployment_mode=os.getenv("DEPLOYMENT_MODE", "standalone"),
            backend_url=os.getenv("BACKEND_URL"),
            db_config=DatabaseConfig(
                url=os.getenv("DATABASE_URL"),
                pool_size=int(os.getenv("DB_POOL_SIZE", "10"))
            ),
            s3_config=S3Config(
                bucket_name=os.getenv("S3_BUCKET"),
                region=os.getenv("AWS_REGION", "us-east-1")
            ),
            athena_config=AthenaConfig(
                database=os.getenv("ATHENA_DATABASE"),
                output_bucket=os.getenv("ATHENA_OUTPUT_BUCKET")
            ),
            flink_config=FlinkConfig(
                jobmanager_url=os.getenv("FLINK_JOBMANAGER_URL"),
                jar_path=os.getenv("FLINK_MERGE_JAR_PATH")
            )
        )
