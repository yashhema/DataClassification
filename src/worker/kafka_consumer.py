# src/worker/kafka_consumer.py
"""
Kafka consumer for receiving configuration update notifications.
Uses aiokafka for async operations and subscribes to worker control topic.

CRITICAL: This uses aiokafka (async), NOT kafka-python (sync).
Workers listen for config updates and fetch new configs via backend API.
"""

import asyncio
import json
import ssl
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError, KafkaConnectionError

from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler, NetworkError, ErrorType
from core.interfaces.worker_backend_interface import IWorkerBackend


class KafkaControlConsumer:
    """
    Async Kafka consumer for receiving configuration update notifications.
    
    Subscribes to control topic partitioned by NodeGroup. When a notification
    arrives, worker calls backend API to fetch the actual updated configuration.
    
    Flow:
    1. Worker subscribes to control topic (partition determined by NodeGroup)
    2. Orchestrator sends config update notification to topic
    3. Consumer receives message with api_endpoint and api_parameters
    4. Worker calls backend API to get actual config
    5. Worker applies new configuration
    
    Usage:
        consumer = KafkaControlConsumer(
            kafka_config=config.kafka,
            node_group="production-us-east",
            backend=worker.backend,
            logger=logger,
            error_handler=error_handler
        )
        
        # Register callback for config updates
        consumer.register_callback("datasource_config", handle_datasource_update)
        
        await consumer.start()
        # Consumer runs in background
        await consumer.stop()
    """
    
    def __init__(
        self,
        kafka_config: "KafkaConfig",
        node_group: str,
        backend: IWorkerBackend,
        logger: SystemLogger,
        error_handler: ErrorHandler
    ):
        """
        Initialize Kafka control topic consumer.
        
        Args:
            kafka_config: Complete Kafka configuration
            node_group: Worker's node group (used for partition assignment)
            backend: Worker backend for API calls
            logger: System logger instance
            error_handler: Error handler instance
        """
        self.config = kafka_config
        self.node_group = node_group
        self.backend = backend
        self.logger = logger
        self.error_handler = error_handler
        
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._is_running = False
        self._consume_task: Optional[asyncio.Task] = None
        
        # Callback handlers for different notification types
        self._callbacks: Dict[str, Callable] = {}
    
    def _create_ssl_context(self) -> ssl.SSLContext:
        """
        Creates SSL context for mTLS connection to Kafka.
        
        Returns:
            Configured SSLContext
        
        Raises:
            NetworkError: If SSL configuration fails
        """
        try:
            ssl_context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=self.config.mtls.ssl_cafile
            )
            
            ssl_context.load_cert_chain(
                certfile=self.config.mtls.ssl_certfile,
                keyfile=self.config.mtls.ssl_keyfile
            )
            
            ssl_context.check_hostname = self.config.mtls.ssl_check_hostname
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            
            return ssl_context
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "kafka_consumer_create_ssl_context",
                operation="ssl_initialization"
            )
            raise NetworkError(
                f"Failed to create Kafka SSL context: {str(e)}",
                ErrorType.NETWORK_CONNECTION_FAILED,
                error_id=error.error_id
            )
    
    async def _get_consumer_offset_from_backend(self) -> Optional[int]:
        """
        Fetches the consumer offset from backend API.
        
        This allows workers to resume from where they left off if they restart.
        Backend tracks the last processed offset per NodeGroup.
        
        Returns:
            Offset to start from, or None to use auto_offset_reset
        
        Note: This is optional - if backend doesn't implement this endpoint,
        consumer will use auto_offset_reset setting from config.
        """
        try:
            context = {
                "node_group": self.node_group,
                "operation": "get_consumer_offset"
            }
            
            # This would call a backend endpoint like:
            # GET /api/v1/workers/consumer-offset?node_group=X&topic=Y
            # For now, we'll return None and use auto_offset_reset
            # TODO: Implement backend endpoint for consumer offset tracking
            
            self.logger.debug(
                "Consumer offset fetch from backend not implemented, using auto_offset_reset",
                **context
            )
            return None
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "kafka_consumer_get_offset",
                node_group=self.node_group
            )
            self.logger.warning(
                "Failed to fetch consumer offset from backend, using auto_offset_reset",
                error_id=error.error_id
            )
            return None
    
    async def start(self) -> None:
        """
        Starts the Kafka consumer and begins consuming messages.
        
        Logic:
        1. Create SSL context
        2. Initialize AIOKafkaConsumer
        3. Get starting offset from backend (optional)
        4. Start consumer
        5. Subscribe to control topic
        6. Launch background consume loop
        
        Error Handling:
        - Connection failures logged but don't prevent worker startup
        - Consumer will retry on connection errors
        """
        if not self.config.enabled:
            self.logger.info("Kafka integration disabled in configuration")
            return
        
        try:
            ssl_context = self._create_ssl_context()
            
            # Calculate consumer group ID based on node group
            # This ensures workers in same node group share partition
            group_id = f"{self.config.consumer.group_id}-{self.node_group}"
            
            self.consumer = AIOKafkaConsumer(
                self.config.topics.worker_control,
                bootstrap_servers=self.config.bootstrap_servers,
                security_protocol=self.config.security_protocol,
                ssl_context=ssl_context,
                
                # Consumer group settings
                group_id=group_id,
                auto_offset_reset=self.config.consumer.auto_offset_reset,
                enable_auto_commit=self.config.consumer.enable_auto_commit,
                auto_commit_interval_ms=self.config.consumer.auto_commit_interval_ms,
                session_timeout_ms=self.config.consumer.session_timeout_ms,
                
                # Deserialization
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            )
            
            await self.consumer.start()
            
            # Optionally seek to backend-provided offset
            offset = await self._get_consumer_offset_from_backend()
            if offset is not None:
                # Get partition assignment
                partitions = self.consumer.assignment()
                for partition in partitions:
                    await self.consumer.seek(partition, offset)
                    self.logger.info(
                        f"Consumer seeking to offset {offset}",
                        partition=partition.partition,
                        offset=offset
                    )
            
            self._is_running = True
            
            # Start background consume loop
            self._consume_task = asyncio.create_task(self._consume_loop())
            
            self.logger.info(
                "Kafka control consumer started successfully",
                topic=self.config.topics.worker_control,
                group_id=group_id,
                node_group=self.node_group
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "kafka_consumer_start",
                operation="consumer_initialization",
                node_group=self.node_group
            )
            self.logger.warning(
                "Kafka consumer initialization failed",
                error_id=error.error_id
            )
            # Don't raise - allow worker to start without consumer
    
    async def stop(self) -> None:
        """
        Stops the Kafka consumer and commits offsets.
        
        Call during worker shutdown to ensure clean shutdown.
        """
        self._is_running = False
        
        # Cancel consume task
        if self._consume_task and not self._consume_task.done():
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
        
        # Stop consumer
        if self.consumer:
            try:
                await self.consumer.stop()
                
                self.logger.info("Kafka control consumer stopped gracefully")
                
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    "kafka_consumer_stop",
                    operation="consumer_shutdown"
                )
                self.logger.warning(
                    "Error during Kafka consumer shutdown",
                    error_id=error.error_id
                )
            finally:
                self.consumer = None
    
    def register_callback(
        self,
        notification_type: str,
        callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """
        Registers a callback handler for a specific notification type.
        
        Args:
            notification_type: Type of notification (e.g., "datasource_config", "connector_config")
            callback: Async function to call when notification received
        
        Example:
            async def handle_datasource_update(notification: Dict[str, Any]):
                datasource_id = notification["datasource_id"]
                # Fetch new config from backend
                new_config = await backend.get_datasource_configuration(datasource_id, {})
                # Apply new config
                await update_datasource_config(new_config)
            
            consumer.register_callback("datasource_config", handle_datasource_update)
        """
        self._callbacks[notification_type] = callback
        
        self.logger.info(
            f"Registered callback for notification type: {notification_type}",
            notification_type=notification_type
        )
    
    async def _consume_loop(self) -> None:
        """
        Background loop that consumes messages from control topic.
        
        This runs continuously until stop() is called.
        
        Logic:
        1. Poll for messages
        2. Deserialize message
        3. Validate message structure
        4. Route to appropriate callback
        5. Call backend API if needed
        6. Commit offset
        """
        self.logger.info("Kafka control consumer loop started")
        
        while self._is_running:
            try:
                # Poll for messages (timeout 1 second)
                async for message in self.consumer:
                    try:
                        await self._process_message(message)
                    except Exception as e:
                        error = self.error_handler.handle_error(
                            e,
                            "kafka_consumer_process_message",
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset
                        )
                        self.logger.error(
                            "Failed to process control message",
                            error_id=error.error_id,
                            offset=message.offset
                        )
                        # Continue processing other messages
                        
            except KafkaConnectionError as e:
                error = self.error_handler.handle_error(
                    e,
                    "kafka_consumer_connection_error",
                    node_group=self.node_group
                )
                self.logger.warning(
                    "Kafka connection error in consumer loop, retrying",
                    error_id=error.error_id
                )
                await asyncio.sleep(5)  # Back off before retry
                
            except asyncio.CancelledError:
                self.logger.info("Kafka consumer loop cancelled")
                break
                
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    "kafka_consumer_loop",
                    node_group=self.node_group
                )
                self.logger.error(
                    "Unexpected error in consumer loop",
                    error_id=error.error_id
                )
                await asyncio.sleep(5)  # Back off before retry
        
        self.logger.info("Kafka control consumer loop stopped")
    
    async def _process_message(self, message) -> None:
        """
        Processes a single control message.
        
        Message format:
        {
            "notification_type": "datasource_config",
            "target_node_group": "production-us-east",
            "timestamp_utc": "2025-10-23T14:15:00Z",
            "api_endpoint": "/api/v1/datasources/{datasource_id}/config",
            "api_parameters": {
                "datasource_id": "ds-123"
            },
            "metadata": {
                "change_reason": "User updated scan schedule",
                "changed_by": "user@example.com"
            }
        }
        
        Args:
            message: Kafka message from aiokafka
        """
        try:
            notification = message.value
            
            # Log receipt
            self.logger.info(
                "Received control notification",
                notification_type=notification.get("notification_type"),
                partition=message.partition,
                offset=message.offset,
                target_node_group=notification.get("target_node_group")
            )
            
            # Validate message structure
            if not self._validate_notification(notification):
                self.logger.warning(
                    "Invalid notification structure, skipping",
                    notification=notification
                )
                return
            
            # Check if notification is for this node group
            target_node_group = notification.get("target_node_group")
            if target_node_group and target_node_group != self.node_group:
                self.logger.debug(
                    "Notification not for this node group, skipping",
                    target=target_node_group,
                    current=self.node_group
                )
                return
            
            # Get notification type
            notification_type = notification["notification_type"]
            
            # Check if we have a registered callback
            callback = self._callbacks.get(notification_type)
            if callback:
                # Call registered callback
                await callback(notification)
            else:
                # Default behavior: fetch config from backend API
                await self._default_config_update_handler(notification)
            
        except Exception as e:
            # Already logged in caller
            raise
    
    def _validate_notification(self, notification: Dict[str, Any]) -> bool:
        """
        Validates notification message structure.
        
        Returns:
            True if valid, False otherwise
        """
        required_fields = ["notification_type", "timestamp_utc"]
        
        for field in required_fields:
            if field not in notification:
                self.logger.warning(
                    f"Missing required field in notification: {field}",
                    notification=notification
                )
                return False
        
        return True
    
    async def _default_config_update_handler(self, notification: Dict[str, Any]) -> None:
        """
        Default handler for config update notifications.
        
        Calls backend API to fetch updated configuration.
        
        Args:
            notification: Notification message with api_endpoint and api_parameters
        """
        try:
            notification_type = notification["notification_type"]
            api_endpoint = notification.get("api_endpoint")
            api_parameters = notification.get("api_parameters", {})
            
            if not api_endpoint:
                self.logger.warning(
                    "No api_endpoint in notification, cannot fetch config",
                    notification_type=notification_type
                )
                return
            
            context = {
                "notification_type": notification_type,
                "api_endpoint": api_endpoint
            }
            
            self.logger.info(
                "Fetching updated configuration from backend",
                notification_type=notification_type,
                api_endpoint=api_endpoint,
                **context
            )
            
            # Map notification types to backend methods
            if notification_type == "datasource_config":
                datasource_id = api_parameters.get("datasource_id")
                if datasource_id:
                    config = await self.backend.get_datasource_configuration(
                        datasource_id,
                        context
                    )
                    self.logger.info(
                        "Fetched updated datasource configuration",
                        datasource_id=datasource_id,
                        **context
                    )
                    # TODO: Apply config to running worker
                    
            elif notification_type == "connector_config":
                connector_type = api_parameters.get("connector_type")
                if connector_type:
                    config = await self.backend.get_connector_configuration(
                        connector_type,
                        context
                    )
                    self.logger.info(
                        "Fetched updated connector configuration",
                        connector_type=connector_type,
                        **context
                    )
                    # TODO: Apply config to running worker
                    
            elif notification_type == "classifier_template":
                template_id = api_parameters.get("template_id")
                if template_id:
                    template = await self.backend.get_classifier_template(
                        template_id,
                        context
                    )
                    self.logger.info(
                        "Fetched updated classifier template",
                        template_id=template_id,
                        **context
                    )
                    # TODO: Apply template to running worker
            
            else:
                self.logger.warning(
                    f"Unknown notification type: {notification_type}",
                    notification_type=notification_type
                )
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "kafka_consumer_default_handler",
                notification_type=notification.get("notification_type")
            )
            self.logger.error(
                "Failed to handle config update notification",
                error_id=error.error_id
            )
    
    def is_running(self) -> bool:
        """
        Checks if consumer is currently running.
        
        Returns:
            True if consumer is active
        """
        return self._is_running and self.consumer is not None
