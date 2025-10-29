# worker/metrics_collector.py
"""
MetricsCollector: Collect and aggregate metrics for Worker/WorkerController.

Uses prometheus_client for metric definitions but serializes to dict
for Kafka publishing instead of using HTTP exposition.

Metrics Categories:
- System: CPU, memory, threads, disk
- Application: Tasks processed, duration, errors
- Component: DB pool, Kafka producer health
"""

import asyncio
import psutil
import threading
from typing import Dict, Any, Union, Optional
from datetime import datetime, timezone
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler
from core.models.kafka_messages import ComponentType


class MetricsCollector:
    """
    Collects and aggregates metrics for a component.
    
    Metrics are stored using prometheus_client but serialized to dict
    for Kafka publishing.
    """
    
    def __init__(
        self,
        component_id: str,
        component_type: ComponentType,
        logger: SystemLogger,
        error_handler: ErrorHandler
    ):
        self.component_id = component_id
        self.component_type = component_type
        self.logger = logger
        self.error_handler = error_handler
        
        # System metrics (updated periodically)
        self.cpu_usage = Gauge(
            'component_cpu_usage_percent',
            'CPU usage percentage',
            ['component_id', 'component_type']
        )
        self.memory_usage = Gauge(
            'component_memory_usage_bytes',
            'Memory usage in bytes',
            ['component_id', 'component_type']
        )
        self.memory_usage_percent = Gauge(
            'component_memory_usage_percent',
            'Memory usage percentage',
            ['component_id', 'component_type']
        )
        self.thread_count = Gauge(
            'component_thread_count',
            'Number of threads',
            ['component_id', 'component_type']
        )
        self.disk_usage_percent = Gauge(
            'component_disk_usage_percent',
            'Disk usage percentage for /mnt/kafka-fallback',
            ['component_id', 'component_type']
        )
        
        # Application metrics (updated inline by Worker)
        self.tasks_processed_total = Counter(
            'worker_tasks_processed_total',
            'Total number of tasks processed',
            ['component_id', 'task_type', 'status']
        )
        self.task_duration_seconds = Histogram(
            'worker_task_duration_seconds',
            'Task processing duration in seconds',
            ['component_id', 'task_type'],
            buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800]
        )
        self.task_errors_total = Counter(
            'worker_task_errors_total',
            'Total number of task errors',
            ['component_id', 'task_type', 'error_type']
        )
        
        # Component health metrics
        self.kafka_producer_available = Gauge(
            'component_kafka_producer_available',
            'Kafka producer availability (1=available, 0=unavailable)',
            ['component_id', 'component_type']
        )
        self.db_pool_size = Gauge(
            'component_db_pool_size',
            'Database connection pool size',
            ['component_id', 'component_type']
        )
        self.db_pool_available = Gauge(
            'component_db_pool_available',
            'Available database connections in pool',
            ['component_id', 'component_type']
        )
        
        # Local file writer metrics (for Worker)
        self.local_files_active = Gauge(
            'worker_local_files_active',
            'Number of active local files being written',
            ['component_id']
        )
        self.local_files_bytes_written = Counter(
            'worker_local_files_bytes_written_total',
            'Total bytes written to local files',
            ['component_id']
        )
        
        # Background task for periodic system metrics collection
        self._collection_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        # Process handle for system metrics
        self._process = psutil.Process()
    
    async def start(self) -> None:
        """Start periodic metrics collection."""
        self._is_running = True
        self._collection_task = asyncio.create_task(self._collect_system_metrics_loop())
        
        self.logger.info(
            "MetricsCollector started",
            component_id=self.component_id
        )
    
    async def stop(self) -> None:
        """Stop periodic metrics collection."""
        self._is_running = False
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info(
            "MetricsCollector stopped",
            component_id=self.component_id
        )
    
    async def _collect_system_metrics_loop(self) -> None:
        """Periodically collect system metrics."""
        while self._is_running:
            try:
                await self._collect_system_metrics()
            except Exception as e:
                self.logger.error(
                    "Failed to collect system metrics",
                    error=str(e)
                )
            
            await asyncio.sleep(10)  # Collect every 10 seconds
    
    async def _collect_system_metrics(self) -> None:
        """Collect current system metrics."""
        try:
            # CPU usage (average over 1 second)
            cpu_percent = self._process.cpu_percent(interval=1.0)
            self.cpu_usage.labels(
                component_id=self.component_id,
                component_type=self.component_type.value
            ).set(cpu_percent)
            
            # Memory usage
            mem_info = self._process.memory_info()
            self.memory_usage.labels(
                component_id=self.component_id,
                component_type=self.component_type.value
            ).set(mem_info.rss)  # Resident Set Size
            
            # Memory usage percent
            mem_percent = self._process.memory_percent()
            self.memory_usage_percent.labels(
                component_id=self.component_id,
                component_type=self.component_type.value
            ).set(mem_percent)
            
            # Thread count
            thread_count = threading.active_count()
            self.thread_count.labels(
                component_id=self.component_id,
                component_type=self.component_type.value
            ).set(thread_count)
            
            # Disk usage for /mnt/kafka-fallback (if exists)
            try:
                disk_usage = psutil.disk_usage('/mnt/kafka-fallback')
                self.disk_usage_percent.labels(
                    component_id=self.component_id,
                    component_type=self.component_type.value
                ).set(disk_usage.percent)
            except:
                pass  # Directory may not exist on WorkerController
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_collect_system_metrics",
                component_id=self.component_id
            )
            self.logger.warning(
                "Error collecting system metrics",
                error_id=error.error_id
            )
    
    async def collect_metrics(self) -> Dict[str, Union[int, float, str]]:
        """
        Collect all metrics and return as dict for Kafka publishing.
        
        Returns:
            Dict of metric_name → value
        """
        metrics = {}
        
        try:
            # Parse prometheus metrics
            # generate_latest() returns bytes with all metrics in text format
            prometheus_output = generate_latest().decode('utf-8')
            
            # Parse each line (skip comments and metadata)
            for line in prometheus_output.split('\n'):
                if line.startswith('#') or not line.strip():
                    continue
                
                # Format: metric_name{label1="value1",label2="value2"} value timestamp
                # We'll simplify and just extract metric_name and value
                try:
                    parts = line.split(' ')
                    if len(parts) >= 2:
                        metric_key = parts[0]
                        metric_value = float(parts[1])
                        
                        # Filter to only metrics for this component
                        if self.component_id in metric_key or 'component_' in metric_key:
                            # Simplify key (remove labels for dict key)
                            simple_key = metric_key.split('{')[0]
                            metrics[simple_key] = metric_value
                except:
                    continue
            
            # Add timestamp
            metrics['collection_timestamp'] = datetime.now(timezone.utc).isoformat()
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="collect_metrics",
                component_id=self.component_id
            )
            self.logger.error(
                "Failed to collect metrics",
                error_id=error.error_id
            )
            metrics = {
                "error": "metrics_collection_failed",
                "error_id": error.error_id
            }
        
        return metrics
    
    # ========================================================================
    # PUBLIC METHODS FOR WORKER TO UPDATE APPLICATION METRICS
    # ========================================================================
    
    def record_task_start(self, task_id: str, task_type: str) -> None:
        """Record task start (for duration tracking)."""
        # Store start time in instance variable or dict
        pass  # Implementation depends on how duration is tracked
    
    def record_task_complete(
        self,
        task_type: str,
        status: str,  # "COMPLETED" or "FAILED"
        duration_seconds: float
    ) -> None:
        """Record task completion."""
        # Increment counter
        self.tasks_processed_total.labels(
            component_id=self.component_id,
            task_type=task_type,
            status=status
        ).inc()
        
        # Record duration
        self.task_duration_seconds.labels(
            component_id=self.component_id,
            task_type=task_type
        ).observe(duration_seconds)
    
    def record_task_error(
        self,
        task_type: str,
        error_type: str
    ) -> None:
        """Record task error."""
        self.task_errors_total.labels(
            component_id=self.component_id,
            task_type=task_type,
            error_type=error_type
        ).inc()
    
    def update_kafka_producer_status(self, is_available: bool) -> None:
        """Update Kafka producer availability."""
        self.kafka_producer_available.labels(
            component_id=self.component_id,
            component_type=self.component_type.value
        ).set(1 if is_available else 0)
    
    def update_db_pool_metrics(self, pool_size: int, available: int) -> None:
        """Update database pool metrics."""
        self.db_pool_size.labels(
            component_id=self.component_id,
            component_type=self.component_type.value
        ).set(pool_size)
        
        self.db_pool_available.labels(
            component_id=self.component_id,
            component_type=self.component_type.value
        ).set(available)
    
    def update_local_files_active(self, count: int) -> None:
        """Update count of active local files."""
        self.local_files_active.labels(
            component_id=self.component_id
        ).set(count)
    
    def record_local_file_bytes(self, bytes_written: int) -> None:
        """Record bytes written to local files."""
        self.local_files_bytes_written.labels(
            component_id=self.component_id
        ).inc(bytes_written)