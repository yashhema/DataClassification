# src/worker/local_file_writer.py
"""
Helper class for managing local file writes before Kafka upload.
Implements the "always write locally first" pattern for resilience.

This ensures no data loss if Kafka is unavailable - all findings are written
to a persistent local file first, then uploaded during task completion.
"""

import os
import json
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, AsyncIterator
from datetime import datetime, timezone
from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler, ProcessingError, ErrorType

class LocalFileWriter:
    """
    Manages local JSONL file for a single task's data.
    
    Usage:
        async with LocalFileWriter(task_id, config, logger, error_handler) as writer:
            await writer.append_record(finding_dict)
            await writer.append_record(another_finding)
            # File is automatically flushed and closed on context exit
            
        # Later, for upload:
        summary = await writer.get_summary()
        async for batch in writer.read_for_upload(batch_size):
            await kafka_producer.send(batch)
        await writer.cleanup()  # Delete file after successful upload
    """
    
    def __init__(
        self,
        task_id: str,
        fallback_directory: str,
        max_file_size_mb: int,
        flush_interval_records: int,
        logger: SystemLogger,
        error_handler: ErrorHandler,
        context: Dict[str, Any]
    ):
        """
        Initialize local file writer for a task.
        
        Args:
            task_id: Hex string task identifier (used as filename)
            fallback_directory: Directory for local files (must exist and be writable)
            max_file_size_mb: Maximum file size before failing task
            flush_interval_records: Flush after this many records
            logger: System logger instance
            error_handler: Error handler instance
            context: Logging context
        
        Raises:
            ProcessingError: If fallback directory doesn't exist or isn't writable
        """
        self.task_id = task_id
        self.fallback_directory = Path(fallback_directory)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        self.flush_interval = flush_interval_records
        self.logger = logger
        self.error_handler = error_handler
        self.context = context
        
        # File path: /mnt/kafka-fallback/{task_id_hex}.jsonl
        self.file_path = self.fallback_directory / f"{task_id}.jsonl"
        self.file_handle: Optional[Any] = None
        
        # Counters
        self.records_written = 0
        self.bytes_written = 0
        self.last_flush_time = datetime.now(timezone.utc)
        
        # Validate fallback directory
        self._validate_directory()
    
    def _validate_directory(self):
        """
        Validates that fallback directory exists and is writable.
        
        Raises:
            ProcessingError: If directory invalid
        """
        try:
            if not self.fallback_directory.exists():
                raise ProcessingError(
                    f"Fallback directory does not exist: {self.fallback_directory}",
                    ErrorType.PROCESSING_CRITICAL_FAILURE,
                    fallback_directory=str(self.fallback_directory)
                )
            
            if not self.fallback_directory.is_dir():
                raise ProcessingError(
                    f"Fallback path is not a directory: {self.fallback_directory}",
                    ErrorType.PROCESSING_CRITICAL_FAILURE,
                    fallback_directory=str(self.fallback_directory)
                )
            
            # Test writability
            test_file = self.fallback_directory / ".write_test"
            try:
                test_file.touch()
                test_file.unlink()
            except Exception as e:
                raise ProcessingError(
                    f"Fallback directory not writable: {self.fallback_directory}",
                    ErrorType.PROCESSING_CRITICAL_FAILURE,
                    fallback_directory=str(self.fallback_directory),
                    error=str(e)
                )
                
        except ProcessingError:
            raise
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "local_file_writer_validate_directory",
                **self.context
            )
            raise ProcessingError(
                "Failed to validate fallback directory",
                ErrorType.PROCESSING_CRITICAL_FAILURE,
                error_id=error.error_id
            )
    
    async def __aenter__(self):
        """
        Async context manager entry - opens file for writing.
        
        Returns:
            self
        """
        try:
            # Open file in append mode with UTF-8 encoding
            # Using 'a' mode so we can resume if worker crashes
            self.file_handle = open(
                self.file_path,
                mode='a',
                encoding='utf-8',
                buffering=8192  # 8KB buffer
            )
            
            self.logger.info(
                f"Local file opened for writing: {self.file_path}",
                task_id=self.task_id,
                file_path=str(self.file_path),
                **self.context
            )
            
            return self
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "local_file_writer_open",
                task_id=self.task_id,
                file_path=str(self.file_path),
                **self.context
            )
            raise ProcessingError(
                f"Failed to open local file: {self.file_path}",
                ErrorType.PROCESSING_CRITICAL_FAILURE,
                error_id=error.error_id
            )
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit - closes and flushes file.
        """
        await self.close()
        return False  # Don't suppress exceptions
    
    async def append_record(self, record: Dict[str, Any]) -> None:
        """
        Appends a single record to the local file as a JSON line.
        
        Args:
            record: Dictionary to write (finding, discovery object, etc.)
        
        Logic:
        1. Serialize record to JSON
        2. Write line to file
        3. Update counters
        4. Check file size limit
        5. Flush if interval reached
        
        Raises:
            ProcessingError: If write fails or file size exceeded
        """
        if not self.file_handle:
            raise ProcessingError(
                "File not open - use context manager",
                ErrorType.PROCESSING_CRITICAL_FAILURE,
                task_id=self.task_id
            )
        
        try:
            # Serialize to JSON line
            json_line = json.dumps(record, ensure_ascii=False)
            line_bytes = len(json_line.encode('utf-8'))
            
            # Check file size limit BEFORE writing
            if self.bytes_written + line_bytes > self.max_file_size_bytes:
                raise ProcessingError(
                    f"Task output exceeds maximum file size: {self.max_file_size_bytes} bytes",
                    ErrorType.PROCESSING_CRITICAL_FAILURE,
                    task_id=self.task_id,
                    bytes_written=self.bytes_written,
                    max_bytes=self.max_file_size_bytes
                )
            
            # Write line
            self.file_handle.write(json_line + '\n')
            
            # Update counters
            self.records_written += 1
            self.bytes_written += line_bytes + 1  # +1 for newline
            
            # Flush if interval reached
            if self.records_written % self.flush_interval == 0:
                await self.flush()
                
        except ProcessingError:
            raise
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "local_file_writer_append",
                task_id=self.task_id,
                records_written=self.records_written,
                **self.context
            )
            raise ProcessingError(
                "Failed to write record to local file",
                ErrorType.PROCESSING_CRITICAL_FAILURE,
                error_id=error.error_id
            )
    
    async def flush(self) -> None:
        """
        Flushes buffered data to disk.
        
        Called automatically at flush_interval or can be called manually.
        Critical for minimizing data loss on worker crash.
        """
        if not self.file_handle:
            return
        
        try:
            self.file_handle.flush()
            os.fsync(self.file_handle.fileno())  # Force OS to write to disk
            
            self.last_flush_time = datetime.now(timezone.utc)
            
            self.logger.debug(
                f"Local file flushed: {self.records_written} records, {self.bytes_written} bytes",
                task_id=self.task_id,
                records_written=self.records_written,
                bytes_written=self.bytes_written,
                **self.context
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "local_file_writer_flush",
                task_id=self.task_id,
                **self.context
            )
            # Log but don't raise - flush failures are not critical
            self.logger.warning(
                "Failed to flush local file",
                error_id=error.error_id,
                **self.context
            )
    
    async def close(self) -> None:
        """
        Closes the file handle after final flush.
        """
        if self.file_handle:
            try:
                await self.flush()
                self.file_handle.close()
                
                self.logger.info(
                    f"Local file closed: {self.records_written} records written",
                    task_id=self.task_id,
                    records_written=self.records_written,
                    bytes_written=self.bytes_written,
                    **self.context
                )
                
            except Exception as e:
                error = self.error_handler.handle_error(
                    e,
                    "local_file_writer_close",
                    task_id=self.task_id,
                    **self.context
                )
                self.logger.warning(
                    "Error closing local file",
                    error_id=error.error_id,
                    **self.context
                )
            finally:
                self.file_handle = None
    
    async def get_summary(self) -> Dict[str, Any]:
        """
        Gets summary statistics about the written file.
        
        Returns:
            Dict with record count, byte count, file path
        
        Used for:
        - Task completion result_payload
        - Logging
        - Monitoring
        """
        file_size = 0
        if self.file_path.exists():
            file_size = self.file_path.stat().st_size
        
        return {
            "task_id": self.task_id,
            "records_written": self.records_written,
            "bytes_written": self.bytes_written,
            "file_size_bytes": file_size,
            "file_path": str(self.file_path),
            "file_exists": self.file_path.exists()
        }
    
    async def read_for_upload(self, batch_size_bytes: int) -> AsyncIterator[list]:
        """
        Reads file content in batches for Kafka upload.
        
        Args:
            batch_size_bytes: Maximum size for each batch (e.g., 900KB)
        
        Yields:
            Lists of records (dicts) that fit within batch_size_bytes
        
        Logic:
        1. Open file for reading
        2. Read lines and deserialize JSON
        3. Accumulate records until batch size reached
        4. Yield batch
        5. Continue until EOF
        
        Note: This is a generator to avoid loading entire file into memory
        """
        if not self.file_path.exists():
            raise ProcessingError(
                f"Local file does not exist for reading: {self.file_path}",
                ErrorType.PROCESSING_CRITICAL_FAILURE,
                task_id=self.task_id,
                file_path=str(self.file_path)
            )
        
        try:
            current_batch = []
            current_batch_bytes = 0
            
            with open(self.file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, start=1):
                    try:
                        # Deserialize JSON line
                        record = json.loads(line.strip())
                        record_bytes = len(line.encode('utf-8'))
                        
                        # Check if adding this record would exceed batch size
                        if current_batch and (current_batch_bytes + record_bytes > batch_size_bytes):
                            # Yield current batch
                            yield current_batch
                            current_batch = []
                            current_batch_bytes = 0
                        
                        # Add record to current batch
                        current_batch.append(record)
                        current_batch_bytes += record_bytes
                        
                    except json.JSONDecodeError as e:
                        # Log corrupted line but continue
                        self.logger.warning(
                            f"Corrupted JSON line in local file: line {line_num}",
                            task_id=self.task_id,
                            line_num=line_num,
                            error=str(e),
                            **self.context
                        )
                        continue
            
            # Yield final batch if any records remain
            if current_batch:
                yield current_batch
                
            self.logger.info(
                f"Completed reading local file for upload",
                task_id=self.task_id,
                records_read=self.records_written,
                **self.context
            )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "local_file_writer_read_for_upload",
                task_id=self.task_id,
                file_path=str(self.file_path),
                **self.context
            )
            raise ProcessingError(
                "Failed to read local file for upload",
                ErrorType.PROCESSING_CRITICAL_FAILURE,
                error_id=error.error_id
            )
    
    async def cleanup(self) -> None:
        """
        Deletes the local file after successful upload.
        
        Call this ONLY after confirming Kafka upload succeeded.
        If upload failed, keep file for manual recovery.
        """
        try:
            if self.file_path.exists():
                self.file_path.unlink()
                
                self.logger.info(
                    f"Local file deleted after successful upload",
                    task_id=self.task_id,
                    file_path=str(self.file_path),
                    **self.context
                )
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "local_file_writer_cleanup",
                task_id=self.task_id,
                file_path=str(self.file_path),
                **self.context
            )
            # Log but don't raise - cleanup failure is not critical
            self.logger.warning(
                "Failed to delete local file after upload",
                error_id=error.error_id,
                **self.context
            )
    
    def get_file_path(self) -> Path:
        """Returns the file path for external access."""
        return self.file_path
    
    def get_records_written(self) -> int:
        """Returns the count of records written."""
        return self.records_written
    
    def get_bytes_written(self) -> int:
        """Returns the count of bytes written."""
        return self.bytes_written
