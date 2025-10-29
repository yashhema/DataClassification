# src/worker/kafka_producer.py
"""
Kafka producer for sending scan results to central cluster.
Uses aiokafka for async operations and implements upload-during-completion pattern.

CRITICAL: This uses aiokafka (async), NOT kafka-python (sync).
Workers write to local files first, then upload during task completion.
"""

import asyncio
import json
import ssl
from uuid import uuid4
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaConnectionError, KafkaTimeoutError

from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler, NetworkError, ProcessingError, ErrorType
from worker.local_file_writer import LocalFileWriter


class KafkaResultsProducer:
    """
    Async Kafka producer for scan findings, discovery metadata, and task outputs.
    
    Implements the "always write locally first" pattern:
    1. During task processing: Write to local file
    2. During task completion: Upload entire file to Kafka
    3. Mark task COMPLETED only after successful upload
    
    Usage:
        producer = KafkaResultsProducer(kafka_config, logger, error_handler)
        await producer.start()
        
        # Upload local file during task completion
        success = await producer.upload_file_to_kafka(
            file_writer=local_file_writer,
            topic_type="findings",
            datasource_id="ds-123",
            job_id=456,
            task_id="abc...",
            context={...}
        )
        
        await producer.stop()
    """
    
    def __init__(
        self,
        kafka_config: "KafkaConfig",  # From configuration_manager
        logger: SystemLogger,
        error_handler: ErrorHandler
    ):
        """
        Initialize Kafka producer with mTLS configuration.
        
        Args:
            kafka_config: Complete Kafka configuration
            logger: System logger instance
            error_handler: Error handler instance
        
        Note: Producer is not started until start() is called.
        This allows for lazy initialization and proper error handling.
        """
        self.config = kafka_config
        self.logger = logger
        self.error_handler = error_handler
        
        self.producer: Optional[AIOKafkaProducer] = None
        self._is_connected = False
        self._connection_failures = 0
        self._max_connection_failures = 3
        
        # Message sequence tracking per task
        self._task_sequences: Dict[str, int] = {}
    
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
                "kafka_producer_create_ssl_context",
                operation="ssl_initialization"
            )
            raise NetworkError(
                f"Failed to create Kafka SSL context: {str(e)}",
                ErrorType.NETWORK_CONNECTION_FAILED,
                error_id=error.error_id
            )
    
    async def start(self) -> None:
        """
        Starts the Kafka producer connection.
        
        Logic:
        1. Create SSL context for mTLS
        2. Initialize AIOKafkaProducer
        3. Start connection
        
        Error Handling:
        - Connection failures logged but don't prevent worker startup
        - Producer will retry on first send attempt
        """
        if not self.config.enabled:
            self.logger.info("Kafka integration disabled in configuration")
            return
        
        try:
            ssl_context = self._create_ssl_context()
            
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                security_protocol=self.config.security_protocol,
                ssl_context=ssl_context,
                
                # Serialization
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                
                # Producer settings for durability
                acks=self.config.producer.acks,
                compression_type=self.config.producer.compression_type,
                request_timeout_ms=self.config.producer.request_timeout_ms,
                enable_idempotence=self.config.producer.enable_idempotence,
                linger_ms=self.config.producer.linger_ms,
                max_in_flight_requests_per_connection=self.config.producer.max_in_flight_requests_per_connection,
                
                # Retry settings
                retries=self.config.resilience.max_retries,
            )
            
            await self.producer.start()
            self._is_connected = True
            self._connection_failures = 0
            
            self.logger.info(
                "Kafka producer started successfully",
                bootstrap_servers=self.config.bootstrap_servers,
                security_protocol=self.config.security_protocol
            )
            
        except Exception as e:
            self._connection_failures += 1
            error = self.error_handler.handle_error(
                e,
                "kafka_producer_start",
                operation="producer_initialization",
                connection_failures=self._connection_failures
            )
            self.logger.warning(
                "Kafka producer initialization failed - will retry on first send",
                error_id=error.error_id,
                connection_failures=self._connection_failures
            )
            # Don't raise - allow worker to start, producer will retry on first send
    
    async def stop(self) -> None:
        """
        Stops the Kafka producer and flushes pending messages.
        
        Call during worker shutdown to ensure all messages are sent.
        """
        if self.producer:
            try:
                await self.producer.flush()
                await self.producer.stop()
                
                self.logger.info("Kafka producer stopped gracefully")
                
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    "kafka_producer_stop",
                    operation="producer_shutdown"
                )
                self.logger.warning(
                    "Error during Kafka producer shutdown",
                    error_id=error.error_id
                )
            finally:
                self.producer = None
                self._is_connected = False
    
    def _create_message_envelope(
        self,
        event_type: str,
        payload: List[Dict[str, Any]],
        job_id: int,
        task_id: str,
        datasource_id: str,
        worker_id: str,
        trace_id: Optional[str],
        sequence: int,
        is_final: bool,
        total_messages: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Creates standardized message envelope for Kafka messages.
        
        Args:
            event_type: e.g., "CLASSIFICATION_FINDINGS", "DISCOVERED_OBJECTS"
            payload: Array of actual data records
            job_id: Job identifier
            task_id: Task identifier (hex string)
            datasource_id: Datasource identifier (used for partitioning)
            worker_id: Worker identifier
            trace_id: Trace identifier for correlation
            sequence: Message sequence number (1, 2, 3...)
            is_final: True if this is the last message for the task
            total_messages: Total message count (only on final message)
        
        Returns:
            Complete message envelope with metadata and payload
        """
        envelope = {
            "metadata": {
                "message_id": str(uuid4()),
                "event_type": event_type,
                "schema_version": "1.0",
                "job_id": job_id,
                "task_id": task_id,
                "trace_id": trace_id,
                "worker_id": worker_id,
                "datasource_id": datasource_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "record_count": len(payload),
                "message_sequence": sequence,
                "is_final_message": is_final
            },
            "payload": payload
        }
        
        # Add total_messages_in_task only on final message
        if is_final and total_messages is not None:
            envelope["metadata"]["total_messages_in_task"] = total_messages
        
        return envelope
    
    async def _send_message(
        self,
        topic: str,
        message: Dict[str, Any],
        partition_key: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Sends a single message to Kafka with retry logic.
        
        Args:
            topic: Kafka topic name
            message: Complete message envelope
            partition_key: Key for partitioning (typically datasource_id)
            context: Logging context
        
        Raises:
            NetworkError: If all retries fail
        """
        if not self.producer or not self._is_connected:
            # Try to reconnect
            await self.start()
            
            if not self.producer or not self._is_connected:
                raise NetworkError(
                    "Kafka producer unavailable after reconnection attempt",
                    ErrorType.NETWORK_CONNECTION_FAILED
                )
        
        max_retries = self.config.resilience.max_retries
        
        for attempt in range(max_retries):
            try:
                # Send message
                future = await self.producer.send(
                    topic=topic,
                    value=message,
                    key=partition_key
                )
                
                # Wait for acknowledgment
                record_metadata = await future
                
                self.logger.info(
                    f"Message sent to Kafka successfully",
                    topic=topic,
                    partition=record_metadata.partition,
                    offset=record_metadata.offset,
                    message_id=message["metadata"]["message_id"],
                    record_count=message["metadata"]["record_count"],
                    **context
                )
                
                return  # Success
                
            except (KafkaConnectionError, KafkaTimeoutError) as e:
                if attempt == max_retries - 1:
                    # Final retry failed
                    error = self.error_handler.handle_error(
                        e,
                        "kafka_producer_send_message",
                        attempt=attempt + 1,
                        topic=topic,
                        **context
                    )
                    raise NetworkError(
                        f"Failed to send message to Kafka after {max_retries} attempts",
                        ErrorType.NETWORK_CONNECTION_FAILED,
                        error_id=error.error_id
                    )
                
                # Exponential backoff
                backoff_seconds = self.config.resilience.retry_backoff_base ** attempt
                self.logger.warning(
                    f"Kafka send failed, retrying in {backoff_seconds}s",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    topic=topic,
                    **context
                )
                await asyncio.sleep(backoff_seconds)
                
            except KafkaError as e:
                # Non-retryable Kafka error
                error = self.error_handler.handle_error(
                    e,
                    "kafka_producer_send_message",
                    topic=topic,
                    **context
                )
                raise NetworkError(
                    f"Kafka error: {str(e)}",
                    ErrorType.NETWORK_CONNECTION_FAILED,
                    error_id=error.error_id
                )
    
    async def upload_file_to_kafka(
        self,
        file_writer: LocalFileWriter,
        topic_type: str,
        datasource_id: str,
        job_id: int,
        task_id: str,
        worker_id: str,
        trace_id: Optional[str],
        context: Dict[str, Any]
    ) -> bool:
        """
        Uploads entire local file content to Kafka in batches.
        
        This is the CORE method implementing the upload-during-completion pattern.
        Called ONLY after task processing is complete.
        
        Args:
            file_writer: LocalFileWriter with completed file
            topic_type: Type of topic ("findings", "discovery", "task_outputs")
            datasource_id: Datasource ID (used for partition key)
            job_id: Job identifier
            task_id: Task identifier (hex string)
            worker_id: Worker identifier
            trace_id: Trace identifier
            context: Logging context
        
        Returns:
            True if upload succeeded, False if failed
        
        Logic:
        1. Get topic name from config
        2. Read file in batches
        3. Create message envelope for each batch
        4. Send each batch with sequence number
        5. Mark final message with is_final_message=true
        6. Track all messages sent
        
        Error Handling:
        - Any failure during upload returns False
        - Task should be marked FAILED if this returns False
        - Local file is NOT deleted on failure (for recovery)
        """
        # Map topic_type to actual topic name
        topic_map = {
            "findings": self.config.topics.findings,
            "discovery": self.config.topics.discovery,
            "task_outputs": self.config.topics.task_outputs,
            "object_metadata": self.config.topics.object_metadata
        }
        
        topic = topic_map.get(topic_type)
        if not topic:
            self.logger.error(
                f"Invalid topic_type: {topic_type}",
                topic_type=topic_type,
                **context
            )
            return False
        
        # Map topic_type to event_type
        event_type_map = {
            "findings": "CLASSIFICATION_FINDINGS",
            "discovery": "DISCOVERED_OBJECTS",
            "task_outputs": "TASK_OUTPUT_RECORD",
            "object_metadata": "OBJECT_METADATA"
        }
        event_type = event_type_map.get(topic_type, topic_type.upper())
        
        try:
            # Reset sequence for this task
            sequence = 1
            total_messages_sent = 0
            
            self.logger.info(
                f"Starting Kafka upload from local file",
                task_id=task_id,
                topic=topic,
                file_path=str(file_writer.get_file_path()),
                **context
            )
            
            # Read file in batches and send
            batches = []
            async for batch in file_writer.read_for_upload(
                self.config.resilience.batch_size_bytes
            ):
                batches.append(batch)
            
            total_batches = len(batches)
            
            for idx, batch in enumerate(batches, start=1):
                is_final = (idx == total_batches)
                
                # Create message envelope
                message = self._create_message_envelope(
                    event_type=event_type,
                    payload=batch,
                    job_id=job_id,
                    task_id=task_id,
                    datasource_id=datasource_id,
                    worker_id=worker_id,
                    trace_id=trace_id,
                    sequence=sequence,
                    is_final=is_final,
                    total_messages=total_batches if is_final else None
                )
                
                # Send to Kafka
                await self._send_message(
                    topic=topic,
                    message=message,
                    partition_key=datasource_id,
                    context=context
                )
                
                sequence += 1
                total_messages_sent += 1
            
            # Optionally send to metadata topic
            if self.config.topics.task_metadata:
                await self._send_task_metadata(
                    job_id=job_id,
                    task_id=task_id,
                    datasource_id=datasource_id,
                    worker_id=worker_id,
                    summary=await file_writer.get_summary(),
                    total_messages=total_messages_sent,
                    context=context
                )
            
            self.logger.info(
                f"Kafka upload completed successfully",
                task_id=task_id,
                topic=topic,
                total_messages=total_messages_sent,
                total_records=file_writer.get_records_written(),
                **context
            )
            
            return True
            
        except NetworkError:
            # Already logged in _send_message
            return False
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "kafka_producer_upload_file",
                task_id=task_id,
                topic=topic,
                **context
            )
            self.logger.error(
                f"Failed to upload file to Kafka",
                error_id=error.error_id,
                task_id=task_id,
                **context
            )
            return False
    
    async def _send_task_metadata(
        self,
        job_id: int,
        task_id: str,
        datasource_id: str,
        worker_id: str,
        summary: Dict[str, Any],
        total_messages: int,
        context: Dict[str, Any]
    ) -> None:
        """
        Sends task completion metadata to optional metadata topic.
        
        This is a small summary message useful for monitoring and dashboards.
        """
        try:
            metadata_message = {
                "metadata": {
                    "message_id": str(uuid4()),
                    "event_type": "TASK_UPLOAD_COMPLETE",
                    "schema_version": "1.0",
                    "job_id": job_id,
                    "task_id": task_id,
                    "datasource_id": datasource_id,
                    "worker_id": worker_id,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat()
                },
                "payload": {
                    "final_status": "UPLOAD_SUCCESSFUL",
                    "total_messages_sent": total_messages,
                    "total_records": summary["records_written"],
                    "total_bytes": summary["bytes_written"]
                }
            }
            
            await self._send_message(
                topic=self.config.topics.task_metadata,
                message=metadata_message,
                partition_key=datasource_id,
                context=context
            )
            
        except Exception as e:
            # Log but don't fail - metadata topic is optional
            self.error_handler.handle_error(
                e,
                "kafka_producer_send_task_metadata",
                task_id=task_id,
                **context
            )
            self.logger.warning(
                "Failed to send task metadata (non-critical)",
                task_id=task_id,
                **context
            )
    
    def is_available(self) -> bool:
        """
        Checks if Kafka producer is available.
        
        Returns:
            True if producer is connected and ready
        """
        return self.producer is not None and self._is_connected
    
    def should_circuit_break(self) -> bool:
        """
        Checks if circuit breaker should open due to connection failures.
        
        Returns:
            True if too many connection failures occurred
        """
        return self._connection_failures >= self._max_connection_failures
