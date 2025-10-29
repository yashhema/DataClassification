# worker_controller/worker_controller.py
"""
WorkerController: Datacenter diagnostic and operations service.

Runs alongside workers in each datacenter/nodegroup.
Handles diagnostic commands without impacting worker task processing.

Responsibilities:
- Test datasource connections
- Test backend/Redis connections
- Collect and upload worker logs
- Run network diagnostics (traceroute, port checks)
"""

import asyncio
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from uuid import uuid4

from core.config.configuration_manager import WorkerControllerConfig
from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler
from core.models.kafka_messages import (
    ComponentType,
    ControlCommandType,
    WorkerControlMessage,
    ControlResponseMessage,
    ControlResponseMetadata,
    ComponentMetricsHealthMessage,
    MetricsHealthMetadata,
    RegistrationEventType
)
from worker.kafka_control_consumer import KafkaControlConsumer
from worker.kafka_metrics_producer import KafkaMetricsProducer
from worker.kafka_response_producer import KafkaResponseProducer
from worker.metrics_collector import MetricsCollector
from worker.health_monitor import HealthMonitor


class WorkerController:
    """
    Datacenter diagnostic and operations service.
    
    Handles commands that require datacenter-local execution
    without interfering with worker task processing.
    """
    
    def __init__(
        self,
        settings: WorkerControllerConfig,
        backend: "APIBackend",
        logger: SystemLogger,
        error_handler: ErrorHandler
    ):
        self.settings = settings
        self.backend = backend
        self.logger = logger
        self.error_handler = error_handler
        
        # Component identity
        self.component_id = f"controller-{uuid4().hex[:8]}"
        self.nodegroup = settings.nodegroup
        self.version = "1.0.0"
        
        # Operational components
        self.metrics_collector: Optional[MetricsCollector] = None
        self.health_monitor: Optional[HealthMonitor] = None
        self.control_consumer: Optional[KafkaControlConsumer] = None
        self.metrics_producer: Optional[KafkaMetricsProducer] = None
        self.response_producer: Optional[KafkaResponseProducer] = None
        
        # State
        self.is_shutting_down = False
    
    async def run(self) -> None:
        """Main run loop."""
        try:
            # Initialize components
            await self._initialize()
            
            # Register component
            await self._register_component()
            
            # Start background tasks
            heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            metrics_task = asyncio.create_task(self._metrics_loop())
            
            self.logger.info(
                "WorkerController started successfully",
                component_id=self.component_id,
                nodegroup=self.nodegroup
            )
            
            # Main loop (just keep alive, control consumer handles commands)
            while not self.is_shutting_down:
                await asyncio.sleep(10)
            
            # Shutdown
            await self._deregister_component()
            heartbeat_task.cancel()
            metrics_task.cancel()
            await self._shutdown()
            
        except Exception as e:
            self.logger.error(
                "WorkerController run loop failed",
                error=str(e)
            )
            raise
    
    async def _initialize(self) -> None:
        """Initialize all components."""
        # Metrics collector
        self.metrics_collector = MetricsCollector(
            component_id=self.component_id,
            component_type=ComponentType.WORKER_CONTROLLER,
            logger=self.logger,
            error_handler=self.error_handler
        )
        await self.metrics_collector.start()
        
        # Health monitor (simpler for controller, no task health)
        self.health_monitor = HealthMonitor(
            component_id=self.component_id,
            db_interface=None,  # Controller doesn't use DB
            kafka_producer=None,
            backend=self.backend,
            metrics_collector=self.metrics_collector,
            logger=self.logger,
            error_handler=self.error_handler
        )
        await self.health_monitor.start()
        
        # Kafka producers
        self.metrics_producer = KafkaMetricsProducer(
            kafka_config=self.settings.kafka,
            logger=self.logger,
            error_handler=self.error_handler
        )
        await self.metrics_producer.start()
        
        self.response_producer = KafkaResponseProducer(
            kafka_config=self.settings.kafka,
            logger=self.logger,
            error_handler=self.error_handler
        )
        await self.response_producer.start()
        
        # Control consumer
        self.control_consumer = KafkaControlConsumer(
            kafka_config=self.settings.kafka,
            nodegroup=self.nodegroup,
            component_id=self.component_id,
            component_type=ComponentType.WORKER_CONTROLLER,
            command_handler=self._handle_control_command,
            logger=self.logger,
            error_handler=self.error_handler
        )
        await self.control_consumer.start()
    
    async def _handle_control_command(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Handle control command (callback for KafkaControlConsumer).
        
        WorkerController handles:
        - TEST_CONNECTION
        - TEST_DB_CONNECTION
        - REQUEST_LOGS
        - RUN_DIAGNOSTIC
        """
        command_type = message.metadata.command_type
        request_id = message.metadata.request_identifier
        
        self.logger.info(
            f"Received control command: {command_type.value}",
            request_id=request_id,
            command=command_type.value
        )
        
        try:
            if command_type == ControlCommandType.TEST_CONNECTION:
                await self._handle_test_connection(message)
            
            elif command_type == ControlCommandType.TEST_DB_CONNECTION:
                await self._handle_test_db_connection(message)
            
            elif command_type == ControlCommandType.REQUEST_LOGS:
                await self._handle_request_logs(message)
            
            elif command_type == ControlCommandType.RUN_DIAGNOSTIC:
                await self._handle_run_diagnostic(message)
            
            else:
                # Unknown command for WorkerController
                await self._send_error_response(
                    request_id,
                    f"Unknown command for WorkerController: {command_type.value}"
                )
        
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="_handle_control_command",
                command_type=command_type.value,
                request_id=request_id
            )
            await self._send_error_response(
                request_id,
                f"Command execution failed: {str(e)}",
                error_id=error.error_id
            )
    
    async def _handle_test_connection(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Test datasource connection.
        
        Parameters:
            datasource_id: str - ID of datasource to test
        """
        request_id = message.metadata.request_identifier
        datasource_id = message.parameters.get("datasource_id")
        
        if not datasource_id:
            await self._send_error_response(
                request_id,
                "Missing datasource_id parameter"
            )
            return
        
        try:
            # Send working status
            await self._send_status_response(request_id, "Working")
            
            # Get datasource config from backend
            datasource_config = await self.backend.get_datasource_configuration(
                datasource_id,
                context={"test_connection": True}
            )
            
            # Import connector factory
            from connectors.connector_factory import ConnectorFactory
            
            # Create connector
            connector = await ConnectorFactory.get_connector(
                datasource_config,
                logger=self.logger,
                error_handler=self.error_handler
            )
            
            # Test connection
            start_time = datetime.now(timezone.utc)
            await connector.test_connection()
            duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            # Send success response
            await self._send_inline_response(
                request_id,
                {
                    "datasource_id": datasource_id,
                    "status": "success",
                    "message": "Connection successful",
                    "duration_ms": duration_ms,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
            
        except Exception as e:
            await self._send_error_response(
                request_id,
                f"Connection test failed: {str(e)}"
            )
    
    async def _handle_test_db_connection(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Test backend database or Redis connection.
        
        Parameters:
            connection_type: str - "database" or "redis"
        """
        request_id = message.metadata.request_identifier
        connection_type = message.parameters.get("connection_type", "database")
        
        try:
            await self._send_status_response(request_id, "Working")
            
            start_time = datetime.now(timezone.utc)
            
            if connection_type == "database":
                # Test database connection
                result = await self.backend.health_check()
                
            elif connection_type == "redis":
                # Test Redis connection
                result = await self.backend.redis_health_check()
                
            else:
                await self._send_error_response(
                    request_id,
                    f"Unknown connection_type: {connection_type}"
                )
                return
            
            duration_ms = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            
            await self._send_inline_response(
                request_id,
                {
                    "connection_type": connection_type,
                    "status": "success" if result else "failed",
                    "duration_ms": duration_ms,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
            
        except Exception as e:
            await self._send_error_response(
                request_id,
                f"DB connection test failed: {str(e)}"
            )
    
    async def _handle_request_logs(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Collect logs from workers and upload to S3.
        
        Parameters:
            worker_ids: List[str] - Worker IDs to collect logs from (empty = all)
            since_hours: int - Collect logs from last N hours (default 24)
        """
        request_id = message.metadata.request_identifier
        worker_ids = message.parameters.get("worker_ids", [])
        since_hours = message.parameters.get("since_hours", 24)
        
        try:
            await self._send_status_response(request_id, "Working")
            
            # Collect logs (implementation depends on deployment)
            # Option 1: K8s API to get logs from pods
            # Option 2: Shared volume with log files
            # Option 3: SSH to worker nodes
            
            # For this example, assume logs are in /var/log/worker/
            log_dir = Path("/var/log/worker")
            
            # Create temporary archive
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
                archive_path = Path(tmp.name)
            
            with tarfile.open(archive_path, "w:gz") as tar:
                for log_file in log_dir.glob("*.log"):
                    # Filter by time if needed
                    tar.add(log_file, arcname=log_file.name)
            
            # Get presigned URL from backend
            upload_url = await self.backend.get_presigned_upload_url(
                file_type="worker_logs",
                context={"request_id": request_id}
            )
            
            # Upload to S3
            async with aiohttp.ClientSession() as session:
                with open(archive_path, "rb") as f:
                    async with session.put(upload_url, data=f) as resp:
                        if resp.status != 200:
                            raise Exception(f"Upload failed: {resp.status}")
            
            # Clean up
            archive_path.unlink()
            
            # Send response with S3 URL
            await self._send_file_response(
                request_id,
                upload_url.split("?")[0],  # Remove query params
                {
                    "worker_ids": worker_ids or "all",
                    "since_hours": since_hours,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )
            
        except Exception as e:
            await self._send_error_response(
                request_id,
                f"Log collection failed: {str(e)}"
            )
    
    async def _handle_run_diagnostic(
        self,
        message: WorkerControlMessage
    ) -> None:
        """
        Run network diagnostic (traceroute, port check, etc.).
        
        Parameters:
            diagnostic_type: str - "traceroute", "port_check", "ping"
            target: str - Target host/IP
            port: int - Port number (for port_check)
        """
        request_id = message.metadata.request_identifier
        diagnostic_type = message.parameters.get("diagnostic_type")
        target = message.parameters.get("target")
        port = message.parameters.get("port")
        
        try:
            await self._send_status_response(request_id, "Working")
            
            result = {}
            
            if diagnostic_type == "traceroute":
                # Run traceroute
                proc = await asyncio.create_subprocess_exec(
                    "traceroute", target,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                result = {
                    "command": f"traceroute {target}",
                    "output": stdout.decode(),
                    "error": stderr.decode()
                }
            
            elif diagnostic_type == "port_check":
                # Test TCP connection
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(target, port),
                        timeout=5.0
                    )
                    writer.close()
                    await writer.wait_closed()
                    result = {
                        "target": target,
                        "port": port,
                        "status": "open",
                        "message": "Connection successful"
                    }
                except asyncio.TimeoutError:
                    result = {
                        "target": target,
                        "port": port,
                        "status": "filtered",
                        "message": "Connection timeout"
                    }
                except Exception as e:
                    result = {
                        "target": target,
                        "port": port,
                        "status": "closed",
                        "message": str(e)
                    }
            
            elif diagnostic_type == "ping":
                # Run ping
                proc = await asyncio.create_subprocess_exec(
                    "ping", "-c", "4", target,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                result = {
                    "command": f"ping -c 4 {target}",
                    "output": stdout.decode(),
                    "error": stderr.decode()
                }
            
            else:
                await self._send_error_response(
                    request_id,
                    f"Unknown diagnostic_type: {diagnostic_type}"
                )
                return
            
            # Send inline response (small results)
            # For large results, upload to S3 and send file response
            await self._send_inline_response(request_id, result)
            
        except Exception as e:
            await self._send_error_response(
                request_id,
                f"Diagnostic failed: {str(e)}"
            )
    
    # Response helper methods
    
    async def _send_status_response(
        self,
        request_id: str,
        status: str
    ) -> None:
        """Send status-only response."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=request_id,
                response_type="status",
                status=status,
                component_id=self.component_id,
                nodegroup=self.nodegroup
            )
        )
        await self.response_producer.send_response(response)
    
    async def _send_inline_response(
        self,
        request_id: str,
        data: Dict[str, Any]
    ) -> None:
        """Send inline data response."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=request_id,
                response_type="inline",
                status="Finished",
                component_id=self.component_id,
                nodegroup=self.nodegroup
            ),
            response_data=data
        )
        await self.response_producer.send_response(response)
    
    async def _send_file_response(
        self,
        request_id: str,
        s3_url: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Send file (S3) response."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=request_id,
                response_type="file",
                status="Finished",
                component_id=self.component_id,
                nodegroup=self.nodegroup
            ),
            response_data={
                "s3_url": s3_url,
                **metadata
            }
        )
        await self.response_producer.send_response(response)
    
    async def _send_error_response(
        self,
        request_id: str,
        error_message: str,
        error_id: Optional[str] = None
    ) -> None:
        """Send error response."""
        response = ControlResponseMessage(
            metadata=ControlResponseMetadata(
                request_identifier=request_id,
                response_type="status",
                status="FinishedWithError",
                component_id=self.component_id,
                nodegroup=self.nodegroup
            ),
            error_message=error_message,
            response_data={"error_id": error_id} if error_id else None
        )
        await self.response_producer.send_response(response)
    
    # Lifecycle methods (similar to Worker)
    
    async def _register_component(self) -> None:
        """Register component on startup."""
        message = ComponentMetricsHealthMessage(
            metadata=MetricsHealthMetadata(
                event_type=RegistrationEventType.COMPONENT_REGISTERED,
                component_id=self.component_id,
                component_type=ComponentType.WORKER_CONTROLLER,
                nodegroup=self.nodegroup
            ),
            version=self.version,
            capabilities={
                "commands": [
                    "TEST_CONNECTION",
                    "TEST_DB_CONNECTION",
                    "REQUEST_LOGS",
                    "RUN_DIAGNOSTIC"
                ]
            }
        )
        await self.metrics_producer.send_metrics(message)
        
        self.logger.info(
            "WorkerController registered",
            component_id=self.component_id
        )
    
    async def _deregister_component(self) -> None:
        """Deregister component on shutdown."""
        message = ComponentMetricsHealthMessage(
            metadata=MetricsHealthMetadata(
                event_type=RegistrationEventType.COMPONENT_DEREGISTERED,
                component_id=self.component_id,
                component_type=ComponentType.WORKER_CONTROLLER,
                nodegroup=self.nodegroup
            )
        )
        await self.metrics_producer.send_metrics(message)
        
        self.logger.info(
            "WorkerController deregistered",
            component_id=self.component_id
        )
    
    async def _heartbeat_loop(self) -> None:
        """Periodic heartbeat."""
        while not self.is_shutting_down:
            try:
                health_status, health_checks = await self.health_monitor.get_health()
                
                message = ComponentMetricsHealthMessage(
                    metadata=MetricsHealthMetadata(
                        event_type=RegistrationEventType.COMPONENT_HEARTBEAT,
                        component_id=self.component_id,
                        component_type=ComponentType.WORKER_CONTROLLER,
                        nodegroup=self.nodegroup
                    ),
                    health_status=health_status,
                    health_checks=health_checks
                )
                
                await self.metrics_producer.send_metrics(message)
                
            except Exception as e:
                self.logger.error("Heartbeat failed", error=str(e))
            
            await asyncio.sleep(30)
    
    async def _metrics_loop(self) -> None:
        """Periodic metrics snapshot."""
        while not self.is_shutting_down:
            try:
                metrics = await self.metrics_collector.collect_metrics()
                health_status, health_checks = await self.health_monitor.get_health()
                
                message = ComponentMetricsHealthMessage(
                    metadata=MetricsHealthMetadata(
                        event_type=RegistrationEventType.METRICS_SNAPSHOT,
                        component_id=self.component_id,
                        component_type=ComponentType.WORKER_CONTROLLER,
                        nodegroup=self.nodegroup
                    ),
                    metrics=metrics,
                    health_status=health_status,
                    health_checks=health_checks
                )
                
                await self.metrics_producer.send_metrics(message)
                
            except Exception as e:
                self.logger.error("Metrics collection failed", error=str(e))
            
            await asyncio.sleep(60)
    
    async def _shutdown(self) -> None:
        """Shutdown all components."""
        await self.control_consumer.stop()
        await self.metrics_producer.stop()
        await self.response_producer.stop()
        await self.metrics_collector.stop()
        await self.health_monitor.stop()


# Entry point
async def main():
    """WorkerController entry point."""
    from core.config.configuration_manager import load_config
    from core.logging.system_logger import SystemLogger
    from core.errors import ErrorHandler
    from core.backend.api_backend import APIBackend
    
    # Load config
    config = load_config()
    
    # Initialize logger and error handler
    logger = SystemLogger(config.logging)
    error_handler = ErrorHandler(logger)
    
    # Initialize backend
    backend = APIBackend(config.backend, logger, error_handler)
    
    # Create and run controller
    controller = WorkerController(
        settings=config.worker_controller,
        backend=backend,
        logger=logger,
        error_handler=error_handler
    )
    
    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())