# worker/health_monitor.py
"""
HealthMonitor: Monitor component health through periodic checks.

Runs 5 health checks:
1. Database connection
2. Kafka producer availability
3. Backend API connectivity
4. Memory usage
5. Task health (Worker only)

Determines overall health status and provides detailed check results.
"""

import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone

from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler
from core.models.kafka_messages import HealthStatus, HealthCheckResult
from worker.metrics_collector import MetricsCollector


class HealthMonitor:
    """
    Monitors component health through periodic checks.
    
    Provides:
    - Overall health status (HEALTHY, DEGRADED, UNHEALTHY)
    - Detailed check results for each health check
    """
    
    def __init__(
        self,
        component_id: str,
        db_interface: Optional[Any],  # DatabaseInterface or None for WorkerController
        kafka_producer: Optional[Any],  # KafkaResultsProducer or None for WorkerController
        backend: "APIBackend",
        metrics_collector: MetricsCollector,
        logger: SystemLogger,
        error_handler: ErrorHandler
    ):
        self.component_id = component_id
        self.db_interface = db_interface
        self.kafka_producer = kafka_producer
        self.backend = backend
        self.metrics_collector = metrics_collector
        self.logger = logger
        self.error_handler = error_handler
        
        # Health check results (cached)
        self._health_status: HealthStatus = HealthStatus.HEALTHY
        self._health_checks: List[HealthCheckResult] = []
        self._last_check_time: Optional[datetime] = None
        
        # Background task
        self._check_task: Optional[asyncio.Task] = None
        self._is_running = False
        
        # Thresholds
        self.memory_warning_threshold_percent = 80.0
        self.memory_critical_threshold_percent = 95.0
        self.task_timeout_warning_seconds = 600  # 10 minutes
    
    async def start(self) -> None:
        """Start periodic health checks."""
        self._is_running = True
        self._check_task = asyncio.create_task(self._health_check_loop())
        
        self.logger.info(
            "HealthMonitor started",
            component_id=self.component_id
        )
    
    async def stop(self) -> None:
        """Stop periodic health checks."""
        self._is_running = False
        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
        
        self.logger.info(
            "HealthMonitor stopped",
            component_id=self.component_id
        )
    
    async def _health_check_loop(self) -> None:
        """Periodically run all health checks."""
        while self._is_running:
            try:
                await self._run_all_checks()
            except Exception as e:
                self.logger.error(
                    "Failed to run health checks",
                    error=str(e)
                )
            
            await asyncio.sleep(30)  # Check every 30 seconds
    
    async def _run_all_checks(self) -> None:
        """Run all health checks and update status."""
        check_results = []
        
        # 1. Database connection check
        if self.db_interface:
            db_result = await self._check_database()
            check_results.append(db_result)
        
        # 2. Kafka producer check
        if self.kafka_producer:
            kafka_result = await self._check_kafka()
            check_results.append(kafka_result)
        
        # 3. Backend API check
        backend_result = await self._check_backend()
        check_results.append(backend_result)
        
        # 4. Memory usage check
        memory_result = await self._check_memory()
        check_results.append(memory_result)
        
        # 5. Task health check (Worker only)
        if self.db_interface:  # Proxy for "is this a Worker?"
            task_result = await self._check_task_health()
            check_results.append(task_result)
        
        # Update cached results
        self._health_checks = check_results
        self._last_check_time = datetime.now(timezone.utc)
        
        # Determine overall status
        self._health_status = self._determine_overall_status(check_results)
    
    async def _check_database(self) -> HealthCheckResult:
        """Check database connection health."""
        try:
            # Simple query to test connection
            start_time = datetime.now(timezone.utc)
            result = await self.db_interface.execute_query("SELECT 1")
            duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            if result:
                return HealthCheckResult(
                    check_name="database_connection",
                    status=HealthStatus.HEALTHY,
                    message=f"Database connection healthy (query: {duration_ms:.2f}ms)",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"duration_ms": duration_ms}
                )
            else:
                return HealthCheckResult(
                    check_name="database_connection",
                    status=HealthStatus.UNHEALTHY,
                    message="Database query returned no result",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={}
                )
        
        except Exception as e:
            return HealthCheckResult(
                check_name="database_connection",
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                last_checked=datetime.now(timezone.utc).isoformat(),
                metadata={"error": str(e)}
            )
    
    async def _check_kafka(self) -> HealthCheckResult:
        """Check Kafka producer health."""
        try:
            is_available = self.kafka_producer.is_available()
            
            if is_available:
                return HealthCheckResult(
                    check_name="kafka_producer",
                    status=HealthStatus.HEALTHY,
                    message="Kafka producer available",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"producer_connected": True}
                )
            else:
                # Check if circuit breaker tripped
                should_break = self.kafka_producer.should_circuit_break()
                
                if should_break:
                    return HealthCheckResult(
                        check_name="kafka_producer",
                        status=HealthStatus.UNHEALTHY,
                        message="Kafka producer circuit breaker tripped",
                        last_checked=datetime.now(timezone.utc).isoformat(),
                        metadata={"circuit_breaker": True}
                    )
                else:
                    return HealthCheckResult(
                        check_name="kafka_producer",
                        status=HealthStatus.DEGRADED,
                        message="Kafka producer temporarily unavailable",
                        last_checked=datetime.now(timezone.utc).isoformat(),
                        metadata={"producer_connected": False}
                    )
        
        except Exception as e:
            return HealthCheckResult(
                check_name="kafka_producer",
                status=HealthStatus.UNHEALTHY,
                message=f"Kafka producer check failed: {str(e)}",
                last_checked=datetime.now(timezone.utc).isoformat(),
                metadata={"error": str(e)}
            )
    
    async def _check_backend(self) -> HealthCheckResult:
        """Check Backend API connectivity."""
        try:
            # Call backend health endpoint
            start_time = datetime.now(timezone.utc)
            is_healthy = await self.backend.health_check()
            duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            if is_healthy:
                return HealthCheckResult(
                    check_name="backend_api",
                    status=HealthStatus.HEALTHY,
                    message=f"Backend API healthy (latency: {duration_ms:.2f}ms)",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"latency_ms": duration_ms}
                )
            else:
                return HealthCheckResult(
                    check_name="backend_api",
                    status=HealthStatus.UNHEALTHY,
                    message="Backend API health check failed",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={}
                )
        
        except Exception as e:
            return HealthCheckResult(
                check_name="backend_api",
                status=HealthStatus.UNHEALTHY,
                message=f"Backend API check failed: {str(e)}",
                last_checked=datetime.now(timezone.utc).isoformat(),
                metadata={"error": str(e)}
            )
    
    async def _check_memory(self) -> HealthCheckResult:
        """Check memory usage."""
        try:
            # Get metrics from collector
            metrics = await self.metrics_collector.collect_metrics()
            
            # Extract memory usage percent
            memory_percent = metrics.get('component_memory_usage_percent', 0.0)
            
            if memory_percent >= self.memory_critical_threshold_percent:
                return HealthCheckResult(
                    check_name="memory_usage",
                    status=HealthStatus.UNHEALTHY,
                    message=f"Critical memory usage: {memory_percent:.1f}%",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"memory_percent": memory_percent}
                )
            elif memory_percent >= self.memory_warning_threshold_percent:
                return HealthCheckResult(
                    check_name="memory_usage",
                    status=HealthStatus.DEGRADED,
                    message=f"High memory usage: {memory_percent:.1f}%",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"memory_percent": memory_percent}
                )
            else:
                return HealthCheckResult(
                    check_name="memory_usage",
                    status=HealthStatus.HEALTHY,
                    message=f"Memory usage normal: {memory_percent:.1f}%",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"memory_percent": memory_percent}
                )
        
        except Exception as e:
            return HealthCheckResult(
                check_name="memory_usage",
                status=HealthStatus.DEGRADED,
                message=f"Memory check failed: {str(e)}",
                last_checked=datetime.now(timezone.utc).isoformat(),
                metadata={"error": str(e)}
            )
    
    async def _check_task_health(self) -> HealthCheckResult:
        """Check if tasks are completing (Worker only)."""
        try:
            # Query database for recent task completions
            query = """
                SELECT COUNT(*) as count
                FROM Task
                WHERE status IN ('COMPLETED', 'FAILED')
                  AND updated_at > NOW() - INTERVAL 10 MINUTE
            """
            result = await self.db_interface.execute_query(query)
            
            recent_completions = result[0]['count'] if result else 0
            
            if recent_completions > 0:
                return HealthCheckResult(
                    check_name="task_health",
                    status=HealthStatus.HEALTHY,
                    message=f"Tasks completing normally ({recent_completions} in last 10 min)",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"recent_completions": recent_completions}
                )
            else:
                # No recent completions - could be degraded or just idle
                # Check if worker is draining
                # For now, mark as DEGRADED
                return HealthCheckResult(
                    check_name="task_health",
                    status=HealthStatus.DEGRADED,
                    message="No recent task completions (worker may be idle)",
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    metadata={"recent_completions": 0}
                )
        
        except Exception as e:
            return HealthCheckResult(
                check_name="task_health",
                status=HealthStatus.DEGRADED,
                message=f"Task health check failed: {str(e)}",
                last_checked=datetime.now(timezone.utc).isoformat(),
                metadata={"error": str(e)}
            )
    
    def _determine_overall_status(
        self,
        check_results: List[HealthCheckResult]
    ) -> HealthStatus:
        """
        Determine overall health status from individual check results.
        
        Logic:
        - If any UNHEALTHY: overall is UNHEALTHY
        - If any DEGRADED: overall is DEGRADED
        - If all HEALTHY: overall is HEALTHY
        """
        has_unhealthy = any(r.status == HealthStatus.UNHEALTHY for r in check_results)
        has_degraded = any(r.status == HealthStatus.DEGRADED for r in check_results)
        
        if has_unhealthy:
            return HealthStatus.UNHEALTHY
        elif has_degraded:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    async def get_health(self) -> Tuple[HealthStatus, List[HealthCheckResult]]:
        """
        Get current health status and check results.
        
        Returns:
            Tuple of (overall_status, check_results)
        """
        # If checks haven't run yet, run them now
        if not self._health_checks:
            await self._run_all_checks()
        
        return (self._health_status, self._health_checks)
    
    def get_last_check_time(self) -> Optional[datetime]:
        """Get timestamp of last health check."""
        return self._last_check_time