"""
worker/kafka_control_consumer.py

Consumer for worker control commands.
Subscribes to prod_worker_control_topic and filters messages for this worker.
"""

import asyncio
from typing import Dict, Any, Optional, Callable
from aiokafka import AIOKafkaConsumer
import json

from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler
from core.models.kafka_messages import (
    WorkerControlMessage,
    ComponentType
)


class KafkaControlConsumer:
    """
    Kafka consumer for worker control commands.
    
    Subscribes to prod_worker_control_topic.
    Filters messages by:
    - target_nodegroup matches worker's nodegroup
    - target_component_type is None or "worker"
    - target_component_id is None or matches worker_id
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str,  # "prod_worker_control_topic"
        group_id: str,
        nodegroup: str,
        component_id: str,
        component_type: ComponentType,
        logger: SystemLogger,
        error_handler: ErrorHandler,
        command_handler: Callable,  # async function to handle commands
        ssl_config: Optional[Dict[str, Any]] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.group_id = group_id
        self.nodegroup = nodegroup
        self.component_id = component_id
        self.component_type = component_type
        self.logger = logger
        self.error_handler = error_handler
        self.command_handler = command_handler
        self.ssl_config = ssl_config or {}
        
        self.consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start the consumer."""
        try:
            self.consumer = AIOKafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',  # Only new commands
                enable_auto_commit=True,
                **self.ssl_config
            )
            await self.consumer.start()
            
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            
            self.logger.info(
                "KafkaControlConsumer started",
                topic=self.topic_name,
                group_id=self.group_id,
                nodegroup=self.nodegroup
            )
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="kafka_control_consumer_start"
            )
            self.logger.error(
                "Failed to start KafkaControlConsumer",
                error_id=error.error_id
            )
    
    async def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            try:
                await self.consumer.stop()
                self.logger.info("KafkaControlConsumer stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping consumer: {e}")
    
    async def _consume_loop(self) -> None:
        """Main consume loop."""
        while self._running:
            try:
                async for msg in self.consumer:
                    if not self._running:
                        break
                    
                    await self._process_message(msg.value)
                    
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    context="kafka_control_consumer_loop"
                )
                self.logger.error(
                    "Error in control consumer loop",
                    error_id=error.error_id
                )
                await asyncio.sleep(5)  # Backoff before retry
    
    async def _process_message(self, message_dict: Dict[str, Any]) -> None:
        """Process a single control message."""
        try:
            # Parse message
            message = WorkerControlMessage(**message_dict)
            
            # Filter: Check if message is for this component
            if not self._is_message_for_me(message):
                self.logger.debug(
                    "Ignoring control message (not for this component)",
                    target_nodegroup=message.target_nodegroup,
                    target_component_id=message.target_component_id
                )
                return
            
            # Log receipt
            self.logger.info(
                "Received control command",
                command_type=message.metadata.command_type.value,
                request_id=message.metadata.request_identifier,
                issued_by=message.metadata.issued_by
            )
            
            # Handle command
            await self.command_handler(message)
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="kafka_control_consumer_process"
            )
            self.logger.error(
                "Failed to process control message",
                error_id=error.error_id
            )
    
    def _is_message_for_me(self, message: WorkerControlMessage) -> bool:
        """
        Check if this control message is targeted at this component.
        
        Message is for this component if:
        1. target_nodegroup matches our nodegroup
        2. target_component_type is None OR matches our type
        3. target_component_id is None OR matches our ID
        """
        # Check nodegroup
        if message.target_nodegroup != self.nodegroup:
            return False
        
        # Check component type
        if message.target_component_type is not None:
            if message.target_component_type != self.component_type:
                return False
        
        # Check component ID
        if message.target_component_id is not None:
            if message.target_component_id != self.component_id:
                return False
        
        return True
