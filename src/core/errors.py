# core/errors.py
"""
Consistent error hierarchy and handling system.
Fixes Issues #34-37: Error handling problems

FIXES APPLIED:
- Replaced all print statements with proper logging
- Added thread-safe error registry with locks
- Fixed race conditions in LRU eviction logic
- Maintained all previous functionality and logic
"""

import traceback
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Any, Optional, List
import logging
import sys
import asyncio
import json
import sqlalchemy
# =============================================================================
# Error Categories and Types
# =============================================================================

class ErrorCategory(Enum):
    """High-level error categories for classification"""
    NETWORK = "network"
    SECURITY = "security"
    CONFIGURATION = "configuration"
    PROCESSING = "processing"
    RESOURCE = "resource"
    VALIDATION = "validation"
    SYSTEM = "system"
    FATAL_BUG = "fatal_bug" 

class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorType(Enum):
    """Specific error types for detailed classification"""
    # Network errors
    NETWORK_CONNECTION_FAILED = "network_connection_failed"
    NETWORK_TIMEOUT = "network_timeout"
    NETWORK_DNS_RESOLUTION = "network_dns_resolution"
    NETWORK_PROTOCOL_ERROR = "network_protocol_error"
    NETWORK_SMB_ERROR = "network_smb_error"
    NETWORK_RESOURCE_NOT_FOUND = "network_resource_not_found"
    PROCESSING_CONTENT_EXTRACTION_FAILED = "processing_content_extraction_failed"
    PROCESSING_RESOURCE_NOT_FOUND = "processing_resource_not_found"
    
    # Security errors
    RIGHTS_ACCESS_DENIED = "rights_access_denied"
    RIGHTS_AUTHENTICATION_FAILED = "rights_authentication_failed"
    RIGHTS_AUTHORIZATION_FAILED = "rights_authorization_failed"
    RIGHTS_CREDENTIAL_INVALID = "rights_credential_invalid"
    
    # Configuration errors
    CONFIGURATION_MISSING = "configuration_missing"
    CONFIGURATION_INVALID = "configuration_invalid"
    CONFIGURATION_SCHEMA_ERROR = "configuration_schema_error"
    CONFIGURATION_DEPENDENCY_MISSING = "configuration_dependency_missing"
    
    # Processing errors
    PROCESSING_DATA_CORRUPTION = "processing_data_corruption"
    PROCESSING_FORMAT_ERROR = "processing_format_error"
    PROCESSING_LOGIC_ERROR = "processing_logic_error"
    PROCESSING_RESOURCE_EXHAUSTED = "processing_resource_exhausted"
    
    # Resource errors
    RESOURCE_MEMORY_EXHAUSTED = "resource_memory_exhausted"
    RESOURCE_DISK_FULL = "resource_disk_full"
    RESOURCE_FILE_NOT_FOUND = "resource_file_not_found"
    RESOURCE_HANDLE_EXHAUSTED = "resource_handle_exhausted"
    
    # Validation errors
    VALIDATION_SCHEMA_ERROR = "validation_schema_error"
    VALIDATION_DATA_INTEGRITY = "validation_data_integrity"
    VALIDATION_CONSTRAINT_VIOLATION = "validation_constraint_violation"
    VALIDATION_TYPE_ERROR = "validation_type_error"
    
    #compilation errors
    PROGRAMMING_ERROR = "programming_error"
    
    # System errors
    SYSTEM_INTERNAL_ERROR = "system_internal_error"
    
    SYSTEM_SERVICE_UNAVAILABLE = "system_service_unavailable"
    SYSTEM_DEPENDENCY_ERROR = "system_dependency_error"
    SYSTEM_SHUTDOWN_REQUESTED = "system_shutdown_requested"


# =============================================================================
# Base Error Classes
# =============================================================================

class ClassificationError(Exception):
    """
    Base exception for all classification system errors.
    Provides structured error information with context.
    """
    
    def __init__(self, 
                 message: str,
                 error_type: ErrorType,
                 error_category: ErrorCategory = None,
                 severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                 retryable: bool = False,
                 retry_delay_seconds: int = 60,
                 max_retries: int = 3,
                 context: Optional[Dict[str, Any]] = None,
                 cause: Optional[Exception] = None):
        
        super().__init__(message)
        
        # Core error information
        self.message = message
        self.error_type = error_type
        self.error_category = error_category or self._infer_category(error_type)
        self.severity = severity
        
        # Retry information
        self.retryable = retryable
        self.retry_delay_seconds = retry_delay_seconds
        self.max_retries = max_retries
        
        # Context and tracking
        self.context = context or {}
        self.cause = cause
        self.timestamp = datetime.now(timezone.utc)
        self.error_id = f"err_{int(self.timestamp.timestamp() * 1000)}"
        
        # Capture stack trace
        self.stack_trace = traceback.format_exc() if sys.exc_info()[0] else None
        
        # Add cause chain if available
        if cause:
            self.context['caused_by'] = {
                'type': type(cause).__name__,
                'message': str(cause),
                'error_id': getattr(cause, 'error_id', None)
            }
    
    def _infer_category(self, error_type: ErrorType) -> ErrorCategory:
        """Infer error category from error type"""
        type_to_category = {
            ErrorType.NETWORK_CONNECTION_FAILED: ErrorCategory.NETWORK,
            ErrorType.NETWORK_TIMEOUT: ErrorCategory.NETWORK,
            ErrorType.NETWORK_DNS_RESOLUTION: ErrorCategory.NETWORK,
            ErrorType.NETWORK_PROTOCOL_ERROR: ErrorCategory.NETWORK,
            
            ErrorType.RIGHTS_ACCESS_DENIED: ErrorCategory.SECURITY,
            ErrorType.RIGHTS_AUTHENTICATION_FAILED: ErrorCategory.SECURITY,
            ErrorType.RIGHTS_AUTHORIZATION_FAILED: ErrorCategory.SECURITY,
            ErrorType.RIGHTS_CREDENTIAL_INVALID: ErrorCategory.SECURITY,
            
            ErrorType.CONFIGURATION_MISSING: ErrorCategory.CONFIGURATION,
            ErrorType.CONFIGURATION_INVALID: ErrorCategory.CONFIGURATION,
            ErrorType.CONFIGURATION_SCHEMA_ERROR: ErrorCategory.CONFIGURATION,
            ErrorType.CONFIGURATION_DEPENDENCY_MISSING: ErrorCategory.CONFIGURATION,
            
            ErrorType.PROCESSING_DATA_CORRUPTION: ErrorCategory.PROCESSING,
            ErrorType.PROCESSING_FORMAT_ERROR: ErrorCategory.PROCESSING,
            ErrorType.PROCESSING_LOGIC_ERROR: ErrorCategory.PROCESSING,
            ErrorType.PROCESSING_RESOURCE_EXHAUSTED: ErrorCategory.PROCESSING,
            
            ErrorType.RESOURCE_MEMORY_EXHAUSTED: ErrorCategory.RESOURCE,
            ErrorType.RESOURCE_DISK_FULL: ErrorCategory.RESOURCE,
            ErrorType.RESOURCE_FILE_NOT_FOUND: ErrorCategory.RESOURCE,
            ErrorType.RESOURCE_HANDLE_EXHAUSTED: ErrorCategory.RESOURCE,
            
            ErrorType.VALIDATION_SCHEMA_ERROR: ErrorCategory.VALIDATION,
            ErrorType.VALIDATION_DATA_INTEGRITY: ErrorCategory.VALIDATION,
            ErrorType.VALIDATION_CONSTRAINT_VIOLATION: ErrorCategory.VALIDATION,
            ErrorType.VALIDATION_TYPE_ERROR: ErrorCategory.VALIDATION,
            
            ErrorType.SYSTEM_INTERNAL_ERROR: ErrorCategory.SYSTEM,
            ErrorType.SYSTEM_SERVICE_UNAVAILABLE: ErrorCategory.SYSTEM,
            ErrorType.SYSTEM_DEPENDENCY_ERROR: ErrorCategory.SYSTEM,
            ErrorType.SYSTEM_SHUTDOWN_REQUESTED: ErrorCategory.SYSTEM,
        }
        
        return type_to_category.get(error_type, ErrorCategory.SYSTEM)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for serialization"""
        return {
            'error_id': self.error_id,
            'error_type': self.error_type.value,
            'error_category': self.error_category.value,
            'severity': self.severity.value,
            'message': self.message,
            'retryable': self.retryable,
            'retry_delay_seconds': self.retry_delay_seconds,
            'max_retries': self.max_retries,
            'timestamp': self.timestamp.isoformat(),
            'context': self.context,
            'stack_trace': self.stack_trace,
            'class_name': self.__class__.__name__
        }
    
    def should_retry(self, attempt_number: int) -> bool:
        """Determine if error should be retried based on attempt number"""
        return self.retryable and attempt_number < self.max_retries
    
    def add_context(self, key: str, value: Any) -> 'ClassificationError':
        """Add context information to error (fluent interface)"""
        self.context[key] = value
        return self
    
    def __str__(self) -> str:
        """String representation with error ID and context"""
        context_str = ""
        if self.context:
            key_contexts = [f"{k}={v}" for k, v in self.context.items() 
                          if k not in ['caused_by', 'stack_trace']]
            if key_contexts:
                context_str = f" [{', '.join(key_contexts)}]"
        
        return f"[{self.error_id}] {self.message}{context_str}"


# =============================================================================
# Specific Error Classes
# =============================================================================

class NetworkError(ClassificationError):
    """Network-related errors (connection, timeout, DNS, etc.)"""
    
    def __init__(self, message: str, 
                 error_type: ErrorType = ErrorType.NETWORK_CONNECTION_FAILED,
                 host: Optional[str] = None,
                 port: Optional[int] = None,
                 **kwargs):
        
        # Network errors are typically retryable
        kwargs.setdefault('retryable', True)
        kwargs.setdefault('retry_delay_seconds', 30)
        kwargs.setdefault('max_retries', 3)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        
        super().__init__(message, error_type, ErrorCategory.NETWORK, **kwargs)
        
        # Add network-specific context
        if host:
            self.add_context('host', host)
        if port:
            self.add_context('port', port)


class RightsError(ClassificationError):
    """Access and permission errors (authentication, authorization)"""
    
    def __init__(self, message: str,
                 error_type: ErrorType = ErrorType.RIGHTS_ACCESS_DENIED,
                 user: Optional[str] = None,
                 resource: Optional[str] = None,
                 required_permission: Optional[str] = None,
                 **kwargs):
        
        # Rights errors are typically not retryable
        kwargs.setdefault('retryable', False)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        
        super().__init__(message, error_type, ErrorCategory.SECURITY, **kwargs)
        
        # Add security-specific context
        if user:
            self.add_context('user', user)
        if resource:
            self.add_context('resource', resource)
        if required_permission:
            self.add_context('required_permission', required_permission)


class CorruptionError(ClassificationError):
    """File corruption and format errors"""
    
    def __init__(self, message: str,
                 error_type: ErrorType = ErrorType.PROCESSING_DATA_CORRUPTION,
                 file_path: Optional[str] = None,
                 expected_format: Optional[str] = None,
                 **kwargs):
        
        # Corruption errors are not retryable
        kwargs.setdefault('retryable', False)
        kwargs.setdefault('severity', ErrorSeverity.MEDIUM)
        
        super().__init__(message, error_type, ErrorCategory.PROCESSING, **kwargs)
        
        # Add corruption-specific context
        if file_path:
            self.add_context('file_path', file_path)
        if expected_format:
            self.add_context('expected_format', expected_format)


class ProcessingError(ClassificationError):
    """Processing and resource errors"""
    
    def __init__(self, message: str,
                 error_type: ErrorType = ErrorType.PROCESSING_LOGIC_ERROR,
                 operation: Optional[str] = None,
                 **kwargs):
        
        # Processing errors may be retryable depending on type
        if error_type in [ErrorType.PROCESSING_RESOURCE_EXHAUSTED, 
                         ErrorType.RESOURCE_MEMORY_EXHAUSTED]:
            kwargs.setdefault('retryable', True)
            kwargs.setdefault('retry_delay_seconds', 120)
        else:
            kwargs.setdefault('retryable', False)
        
        kwargs.setdefault('severity', ErrorSeverity.MEDIUM)
        
        super().__init__(message, error_type, ErrorCategory.PROCESSING, **kwargs)
        
        # Add processing-specific context
        if operation:
            self.add_context('operation', operation)


class ConfigurationError(ClassificationError):
    """Configuration and setup errors"""
    
    def __init__(self, message: str,
                 error_type: ErrorType = ErrorType.CONFIGURATION_INVALID,
                 config_section: Optional[str] = None,
                 config_key: Optional[str] = None,
                 **kwargs):
        
        # Configuration errors are not retryable
        kwargs.setdefault('retryable', False)
        kwargs.setdefault('severity', ErrorSeverity.HIGH)
        
        super().__init__(message, error_type, ErrorCategory.CONFIGURATION, **kwargs)
        
        # Add configuration-specific context
        if config_section:
            self.add_context('config_section', config_section)
        if config_key:
            self.add_context('config_key', config_key)


class ValidationError(ClassificationError):
    """Data validation and integrity errors"""
    
    def __init__(self, message: str,
                 error_type: ErrorType = ErrorType.VALIDATION_SCHEMA_ERROR,
                 field_name: Optional[str] = None,
                 field_value: Optional[Any] = None,
                 expected_type: Optional[str] = None,
                 **kwargs):
        
        # Validation errors are not retryable
        kwargs.setdefault('retryable', False)
        kwargs.setdefault('severity', ErrorSeverity.MEDIUM)
        
        super().__init__(message, error_type, ErrorCategory.VALIDATION, **kwargs)
        
        # Add validation-specific context
        if field_name:
            self.add_context('field_name', field_name)
        if field_value is not None:
            self.add_context('field_value', str(field_value))
        if expected_type:
            self.add_context('expected_type', expected_type)


# =============================================================================
# Error Handling Utilities
# =============================================================================

class ErrorHandler:
    """Centralized error handling and logging with thread safety"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        
        # FIXED: Thread-safe error registry
        self._registry_lock = threading.Lock()
        self.error_registry: Dict[str, ClassificationError] = {}
        self._max_registry_size = 1000  # Prevent unbounded growth

    def _convert_exception_with_context(self, 
                                      error: Exception, 
                                      context: str,
                                      operation: Optional[str] = None,
                                      **additional_context) -> ClassificationError:
        """Convert generic exception with context-aware classification logic"""
        
        error_message = str(error)
        error_class = type(error).__name__
        
        # SOLUTION: Context-aware TypeError and ValueError handling
        if isinstance(error, (TypeError, ValueError)):
            # Check if error occurred in core system context
            if self._is_core_system_context(context, operation, additional_context):
                return ClassificationError(
                    f"FATAL BUG in core system: {error_class}: {error_message}",
                    ErrorType.PROGRAMMING_ERROR,
                    ErrorCategory.FATAL_BUG,
                    ErrorSeverity.CRITICAL,
                    retryable=False,
                    cause=error,
                    context={'original_context': context, 'operation': operation, **additional_context}
                )
            else:
                # Data processing context - treat as validation error
                return ValidationError(
                    f"Data validation error: {error_class}: {error_message}",
                    ErrorType.VALIDATION_TYPE_ERROR,
                    cause=error,
                    context={'original_context': context, 'operation': operation, **additional_context}
                )
        
        # For all other exception types, use your existing logic
        # (copy the rest from your current _convert_exception method)

    def _is_core_system_context(self, context: str, operation: Optional[str], 
                              additional_context: Dict[str, Any]) -> bool:
        """
        Determine if error occurred in core system context.
        Returns True if error should be treated as fatal programming bug.
        """
        
        # Check context string for core system indicators
        context_lower = context.lower()
        core_indicators = [
            'orchestrator', 'task_manager', 'task_assigner', 'job_monitor',
            'lease_manager', 'state_transition', 'database_interface', 
            'health_check', 'resource_coordinator', 'pipeliner', 'reaper'
        ]
        
        for indicator in core_indicators:
            if indicator in context_lower:
                return True
        
        # Check for data processing indicators (return False)
        data_processing_indicators = [
            'connector', 'classification', 'content_extraction', 
            'file_processing', 'database_content', 'row_processing'
        ]
        
        for indicator in data_processing_indicators:
            if indicator in context_lower:
                return False
        
        # Check additional context for data processing keys
        data_context_keys = {'file_path', 'object_path', 'datasource_id', 'content_type'}
        if any(key in additional_context for key in data_context_keys):
            return False
        
        # Default: treat as core system error for safety
        return True

    def handle_error(self, 
                    error: Exception, 
                    context: str,
                    operation: Optional[str] = None,
                    **additional_context) -> ClassificationError:
        """
        Convert any exception to ClassificationError and handle appropriately
        """
        if not isinstance(error, Exception):
            self.logger.error(f"ERROR HANDLER BUG: Received non-Exception object: {type(error)}")
            # Create a synthetic error to work with
            error = RuntimeError(f"Invalid error object passed to handler: {str(error)}")
        
        if not context:
            self.logger.warning("ERROR HANDLER: Empty context provided, using fallback")
            context = "unknown_context"
        # Log error details
        self.logger.error(f"ERROR in {context}")
        self.logger.error(f"Operation: {operation}")
        self.logger.error(f"Error Type: {type(error).__name__}")
        self.logger.error(f"Error Message: {str(error)}")
        
        # Log the stack trace using logger
        self.logger.error("Stack trace:", exc_info=True)
        
        # Log additional context if present
        if additional_context:
            self.logger.error(f"Additional Context: {additional_context}")

        # Initialize classification_error to None to track assignment
        classification_error = None

        # If it's already a ClassificationError, add context and use it
        if isinstance(error, ClassificationError):
            classification_error = error
            if operation:
                classification_error.add_context('operation', operation)
            classification_error.add_context('context', context)
            for key, value in additional_context.items():
                classification_error.add_context(key, value)
        else:
            # Convert generic exception to ClassificationError
            classification_error = self._convert_exception(error, context, operation, **additional_context)
        
        # CRITICAL FIX: Ensure we always have a valid ClassificationError
        if classification_error is None:
            self.logger.error("ERROR: _convert_exception returned None - creating fallback error")
            classification_error = ClassificationError(
                f"Error handler failed to classify exception: {type(error).__name__}: {str(error)}",
                ErrorType.SYSTEM_INTERNAL_ERROR,
                ErrorCategory.SYSTEM,
                ErrorSeverity.HIGH,
                cause=error,
                context={'original_context': context, 'operation': operation, **additional_context}
            )
        
        # Log the error (with null check)
        self._log_error(classification_error)
        
        # Thread-safe registry management
        with self._registry_lock:
            # Check size limit and evict oldest if needed
            if len(self.error_registry) >= self._max_registry_size:
                try:
                    oldest_id = min(self.error_registry.keys(), 
                                  key=lambda k: self.error_registry[k].timestamp)
                    del self.error_registry[oldest_id]
                except (ValueError, KeyError):
                    # Registry was empty or modified during iteration
                    pass
            
            # Register the new error
            self.error_registry[classification_error.error_id] = classification_error
        
        return classification_error


    def _convert_exception(self, 
                          error: Exception, 
                          context: str,
                          operation: Optional[str] = None,
                          **additional_context) -> ClassificationError:
        """Convert generic exception to appropriate ClassificationError"""
        
        error_message = str(error)
        error_class = type(error).__name__
        
        # Map common exception types to our error types
        if isinstance(error, (ConnectionError, OSError)) and 'connection' in error_message.lower():
            return NetworkError(
                f"{error_class}: {error_message}",
                ErrorType.NETWORK_CONNECTION_FAILED,
                cause=error,
                context={'original_context': context, 'operation': operation, **additional_context}
            )
        
        elif isinstance(error, PermissionError):
            return RightsError(
                f"{error_class}: {error_message}",
                ErrorType.RIGHTS_ACCESS_DENIED,
                cause=error,
                context={'original_context': context, 'operation': operation, **additional_context}
            )
        
        elif isinstance(error, FileNotFoundError):
            return ProcessingError(
                f"{error_class}: {error_message}",
                ErrorType.RESOURCE_FILE_NOT_FOUND,
                cause=error,
                context={'original_context': context, 'operation': operation, **additional_context}
            )
        
        elif isinstance(error, (ValueError, TypeError)):
            return ValidationError(
                f"{error_class}: {error_message}",
                ErrorType.VALIDATION_TYPE_ERROR,
                cause=error,
                context={'original_context': context, 'operation': operation, **additional_context}
            )

        # Add to _convert_exception method:
        elif isinstance(error, (ImportError, ModuleNotFoundError)):
            return ConfigurationError(
                f"Module/import error: {error_class}: {error_message}",
                ErrorType.CONFIGURATION_DEPENDENCY_MISSING,
                cause=error,
                context={'original_context': context, **additional_context}
            )

        elif isinstance(error, (asyncio.TimeoutError, asyncio.CancelledError)):
            return ProcessingError(
                f"Async operation failed: {error_class}: {error_message}",
                ErrorType.PROCESSING_RESOURCE_EXHAUSTED,
                cause=error,
                retryable=True,
                context={'original_context': context, **additional_context}
            )

        elif isinstance(error, json.JSONDecodeError):
            return ValidationError(
                f"JSON parsing failed: {error_message}",
                ErrorType.VALIDATION_SCHEMA_ERROR,
                cause=error,
                context={'original_context': context, **additional_context}
            )

        elif isinstance(error, (NameError, SyntaxError, AttributeError)):
            return ClassificationError(
                f"FATAL BUG: {type(error).__name__}: {str(error)}",
                ErrorType.PROGRAMMING_ERROR, # A new ErrorType would also be added
                ErrorCategory.FATAL_BUG,
                ErrorSeverity.CRITICAL,
                retryable=False,  # Never retry fatal bugs
                cause=error,
                context={'original_context': context, **additional_context}
            )
        
        elif isinstance(error, MemoryError):
            return ProcessingError(
                f"{error_class}: {error_message}",
                ErrorType.RESOURCE_MEMORY_EXHAUSTED,
                cause=error,
                retryable=True,
                context={'original_context': context, 'operation': operation, **additional_context}
            )
        elif isinstance(error, (sqlalchemy.exc.DatabaseError, sqlalchemy.exc.DataError)):
            return ProcessingError(
                f"Database operation failed: {error_class}: {error_message}",
                ErrorType.PROCESSING_LOGIC_ERROR,
                cause=error,
                retryable=True,  # Database format issues might be retryable
                context={'original_context': context, 'operation': operation, **additional_context}
            )        
        else:
            # Generic system error
            return ClassificationError(
                f"{error_class}: {error_message}",
                ErrorType.SYSTEM_INTERNAL_ERROR,
                ErrorCategory.SYSTEM,
                ErrorSeverity.HIGH,
                cause=error,
                context={'original_context': context, 'operation': operation, **additional_context}
            )
    

    def _log_error(self, error: ClassificationError):
        """Log error with appropriate level based on severity"""
        
        # Safety check to prevent cascading errors
        if error is None:
            self.logger.error("CRITICAL: _log_error received None instead of ClassificationError")
            return
        
        error_dict = error.to_dict()
        
        if error.severity == ErrorSeverity.CRITICAL:
            self.logger.critical("Critical error occurred", extra={'error_data': error_dict})
        elif error.severity == ErrorSeverity.HIGH:
            self.logger.error("High severity error occurred", extra={'error_data': error_dict})
        elif error.severity == ErrorSeverity.MEDIUM:
            self.logger.warning("Medium severity error occurred", extra={'error_data': error_dict})
        else:
            self.logger.info("Low severity error occurred", extra={'error_data': error_dict})

    
    def get_error_by_id(self, error_id: str) -> Optional[ClassificationError]:
        """Retrieve error by ID from registry (thread-safe)"""
        with self._registry_lock:
            return self.error_registry.get(error_id)
    
    def get_recent_errors(self, 
                         count: int = 10,
                         severity_filter: Optional[ErrorSeverity] = None,
                         category_filter: Optional[ErrorCategory] = None) -> List[ClassificationError]:
        """Get recent errors with optional filtering (thread-safe)"""
        
        with self._registry_lock:
            errors = list(self.error_registry.values())
        
        # Apply filters
        if severity_filter:
            errors = [e for e in errors if e.severity == severity_filter]
        if category_filter:
            errors = [e for e in errors if e.error_category == category_filter]
        
        # Sort by timestamp (most recent first) and limit
        errors.sort(key=lambda e: e.timestamp, reverse=True)
        return errors[:count]
    
    def clear_old_errors(self, max_age_hours: int = 24):
        """Clear errors older than specified hours (thread-safe)"""
        cutoff_time = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)
        
        with self._registry_lock:
            old_error_ids = [
                error_id for error_id, error in self.error_registry.items()
                if error.timestamp.timestamp() < cutoff_time
            ]
            
            for error_id in old_error_ids:
                del self.error_registry[error_id]


# =============================================================================
# Retry Mechanism
# =============================================================================

class RetryManager:
    """Manages retry logic for retryable errors"""
    
    def __init__(self, error_handler: ErrorHandler):
        self.error_handler = error_handler
        self.retry_attempts: Dict[str, int] = {}
    
    def should_retry(self, error: ClassificationError, operation_id: str) -> bool:
        """Determine if operation should be retried"""
        
        if not error.retryable:
            return False
        
        current_attempts = self.retry_attempts.get(operation_id, 0)
        return error.should_retry(current_attempts)
    
    def record_retry_attempt(self, operation_id: str):
        """Record a retry attempt for an operation"""
        self.retry_attempts[operation_id] = self.retry_attempts.get(operation_id, 0) + 1
    
    def get_retry_delay(self, error: ClassificationError, operation_id: str) -> int:
        """Get delay before next retry (with exponential backoff)"""
        attempt_number = self.retry_attempts.get(operation_id, 0)
        base_delay = error.retry_delay_seconds
        
        # Exponential backoff with jitter
        import random
        delay = base_delay * (2 ** attempt_number)
        jitter = random.uniform(0.8, 1.2)  # ±20% jitter
        
        return int(delay * jitter)
    
    def clear_retry_history(self, operation_id: str):
        """Clear retry history for successful operation"""
        self.retry_attempts.pop(operation_id, None)


# =============================================================================
# Module Testing
# =============================================================================

def test_error_system():
    """Test the error handling system"""
    
    # Test basic error creation
    error = NetworkError(
        "Connection failed to database server",
        ErrorType.NETWORK_CONNECTION_FAILED,
        host="db.example.com",
        port=1433
    )
    
    logger = logging.getLogger("test")
    logger.info(f"Created error: {error}")
    logger.info(f"Error ID: {error.error_id}")
    logger.info(f"Retryable: {error.retryable}")
    logger.info(f"Context: {error.context}")
    
    # Test error conversion
    handler = ErrorHandler()
    try:
        raise ValueError("Invalid configuration value")
    except Exception as e:
        converted_error = handler.handle_error(e, "test_context", operation="test_operation")
        logger.info(f"Converted error: {converted_error}")
        logger.info(f"Error type: {converted_error.error_type}")
    
    # Test serialization
    error_dict = error.to_dict()
    logger.info(f"Serialized error keys: {list(error_dict.keys())}")
    
    logger.info("Error system test completed!")


if __name__ == "__main__":
    test_error_system()