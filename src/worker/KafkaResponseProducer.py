"""
worker/kafka_response_producer.py

Producer for control command responses.
Publishes to prod_worker_response_topic.
"""

from typing import Dict, Any, Optional
from aiokafka import AIOKafkaProducer
import json

from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler
from core.models.kafka_messages import ControlResponseMessage


class KafkaResponseProducer:
    """
    Kafka producer for control command responses.
    
    Publishes ControlResponseMessage to prod_worker_response_topic.
    Partitions by nodegroup for consistent routing.
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic_name: str,  # "prod_worker_response_topic"
        logger: SystemLogger,
        error_handler: ErrorHandler,
        ssl_config: Optional[Dict[str, Any]] = None
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.logger = logger
        self.error_handler = error_handler
        self.ssl_config = ssl_config or {}
        
        self.producer: Optional[AIOKafkaProducer] = None
        self._is_available = False
    
    async def start(self) -> None:
        """Initialize and start the Kafka producer."""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **self.ssl_config
            )
            await self.producer.start()
            self._is_available = True
            self.logger.info(
                "KafkaResponseProducer started",
                topic=self.topic_name
            )
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="kafka_response_producer_start"
            )
            self.logger.error(
                "Failed to start KafkaResponseProducer",
                error_id=error.error_id
            )
            self._is_available = False
    
    async def stop(self) -> None:
        """Stop the Kafka producer."""
        if self.producer:
            try:
                await self.producer.stop()
                self.logger.info("KafkaResponseProducer stopped")
            except Exception as e:
                self.logger.warning(f"Error stopping producer: {e}")
            finally:
                self._is_available = False
    
    def is_available(self) -> bool:
        """Check if producer is ready."""
        return self._is_available and self.producer is not None
    
    async def publish_response(
        self,
        response: ControlResponseMessage,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish a control response message to Kafka.
        
        Args:
            response: ControlResponseMessage to publish
            context: Logging context
            
        Returns:
            True if published successfully, False otherwise
        """
        if not self.is_available():
            self.logger.warning(
                "KafkaResponseProducer not available",
                **(context or {})
            )
            return False
        
        try:
            # Serialize message
            message_dict = response.model_dump(exclude_none=True)
            
            # Use nodegroup as partition key
            partition_key = response.metadata.nodegroup.encode('utf-8')
            
            # Send to Kafka
            await self.producer.send_and_wait(
                self.topic_name,
                value=message_dict,
                key=partition_key
            )
            
            self.logger.debug(
                "Published control response",
                request_id=response.metadata.request_identifier,
                status=response.metadata.status,
                **(context or {})
            )
            
            return True
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                context="kafka_response_producer_publish",
                **(context or {})
            )
            self.logger.error(
                "Failed to publish control response",
                error_id=error.error_id,
                **(context or {})
            )
            return False
