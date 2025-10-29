# src/core/backends/api_backend.py
"""
REST API backend for distributed EKS deployment mode.
Worker communicates with Backend API over HTTP with mTLS.

CORRECTED: report_task_progress uses primitives and notes Kafka routing (Addendum 3.1.1, 3.1.2)
"""
import aiohttp
import asyncio
import ssl
from typing import Optional, Dict, Any
from core.interfaces.worker_backend_interface import IWorkerBackend
from core.models.models import WorkPacket
from core.config.config_models import DataSourceConfiguration
from core.logging.system_logger import SystemLogger
from core.errors import (
    ErrorHandler, NetworkError, ConfigurationError, 
    RightsError, ProcessingError, ErrorType
)

class APIBackend(IWorkerBackend):
    """
    Implementation using HTTP REST API calls to backend service.
    Used when deployment_model = "EKS"
    """
    
    def __init__(
        self,
        backend_api_url: str,  # e.g., "https://backend.example.com"
        api_key: str,          # For authentication
        cert_path: str,        # Client certificate for mTLS
        key_path: str,         # Client private key
        ca_path: str,          # CA cert to verify server
        logger: SystemLogger,
        error_handler: ErrorHandler,
        timeout_seconds: int = 30
    ):
        self.api_url = backend_api_url.rstrip('/')
        self.api_key = api_key
        self.cert_path = cert_path
        self.key_path = key_path
        self.ca_path = ca_path
        self.logger = logger
        self.error_handler = error_handler
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        
        # SSL context for mTLS
        self._ssl_context = self._create_ssl_context()
    
    def _create_ssl_context(self):
        """
        Creates SSL context for mTLS.
        
        Logic:
        1. Load client cert and key
        2. Load CA cert for server verification
        3. Configure for mutual authentication
        
        Error Handling:
        - If cert files missing, raise ConfigurationError at init time
        """
        try:
            ssl_context = ssl.create_default_context(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile=self.ca_path
            )
            ssl_context.load_cert_chain(
                certfile=self.cert_path,
                keyfile=self.key_path
            )
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            return ssl_context
        except Exception as e:
            raise ConfigurationError(
                f"Failed to create mTLS SSL context: {str(e)}",
                ErrorType.CONFIGURATION_INVALID,
                cert_path=self.cert_path
            )
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        context: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Makes HTTP request to backend API with standard error handling.
        
        Logic:
        1. Add API key to headers
        2. Make request with mTLS
        3. Handle common HTTP errors (401, 403, 404, 500)
        4. Parse JSON response
        
        Error Handling:
        - 401: Authentication failed → NetworkError
        - 403: Authorization failed → RightsError
        - 404: Not found → ConfigurationError
        - 500: Server error → NetworkError
        - Timeout: NetworkError
        - Connection error: NetworkError
        
        Returns:
            Parsed JSON response
            
        Raises:
            NetworkError, RightsError, or ConfigurationError
        """
        url = f"{self.api_url}{endpoint}"
        headers = {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json"
        }
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.request(
                    method,
                    url,
                    json=data,
                    headers=headers,
                    ssl=self._ssl_context
                ) as response:
                    # Log request/response for debugging
                    self.logger.debug(
                        f"API request: {method} {endpoint}",
                        status_code=response.status,
                        **(context or {})
                    )
                    
                    # Handle HTTP errors
                    if response.status == 401:
                        raise NetworkError(
                            "API authentication failed - invalid API key",
                            ErrorType.NETWORK_AUTH_FAILED,
                            endpoint=endpoint
                        )
                    elif response.status == 403:
                        raise RightsError(
                            "API authorization failed - insufficient permissions",
                            ErrorType.RIGHTS_AUTHORIZATION_FAILED,
                            endpoint=endpoint
                        )
                    elif response.status == 404:
                        raise ConfigurationError(
                            f"Resource not found: {endpoint}",
                            ErrorType.CONFIGURATION_MISSING,
                            endpoint=endpoint
                        )
                    elif response.status >= 500:
                        raise NetworkError(
                            f"Backend API server error: {response.status}",
                            ErrorType.NETWORK_CONNECTION_FAILED,
                            endpoint=endpoint
                        )
                    
                    response.raise_for_status()
                    return await response.json()
                    
        except aiohttp.ClientError as e:
            raise NetworkError(
                f"HTTP client error: {str(e)}",
                ErrorType.NETWORK_CONNECTION_FAILED,
                endpoint=endpoint
            )
        except asyncio.TimeoutError:
            raise NetworkError(
                f"Request timeout after {self.timeout.total}s",
                ErrorType.NETWORK_TIMEOUT,
                endpoint=endpoint
            )
    
    async def get_next_task(
        self,
        worker_id: str,
        node_group: str
    ) -> Optional[WorkPacket]:
        """
        Fetches next task from backend API.
        
        Endpoint: GET /api/v1/tasks/next?worker_id=X&node_group=Y
        
        Logic:
        1. Make GET request
        2. Backend atomically assigns task to worker
        3. Returns WorkPacket JSON or empty response
        4. Parse JSON into WorkPacket Pydantic model
        
        Error Handling:
        - Network error: Log and return None (worker retries)
        - Parse error: Log and return None
        
        Returns:
            WorkPacket or None
        """
        try:
            endpoint = f"/api/v1/tasks/next?worker_id={worker_id}&node_group={node_group}"
            response = await self._make_request(
                "GET",
                endpoint,
                context={"worker_id": worker_id, "node_group": node_group}
            )
            
            if not response or "task" not in response:
                return None
            
            # Parse into WorkPacket model
            work_packet = WorkPacket.parse_obj(response["task"])
            self.logger.info(
                f"Received task {work_packet.header.task_id}",
                worker_id=worker_id,
                task_type=work_packet.payload.task_type
            )
            return work_packet
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "api_backend_get_next_task",
                worker_id=worker_id,
                node_group=node_group
            )
            self.logger.warning(
                "Failed to fetch task from API",
                error_id=error.error_id
            )
            return None
    
    async def report_task_completion(
        self,
        task_id: str,
        status: str,
        result_payload: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Reports task completion to backend API.
        
        Endpoint: POST /api/v1/tasks/{task_id}/complete
        Body: {"status": "COMPLETED", "result": {...}}
        
        Logic:
        1. Make POST request with retry logic
        2. Backend updates task status in DB
        3. Backend notifies orchestrator (if different process)
        
        Error Handling:
        - Retry 3 times with exponential backoff
        - If all fail, raise ProcessingError (task will be marked failed)
        """
        max_retries = 3
        endpoint = f"/api/v1/tasks/{task_id}/complete"
        data = {
            "status": status,
            "result": result_payload,
            "worker_id": context.get("worker_id")
        }
        
        for attempt in range(max_retries):
            try:
                await self._make_request(
                    "POST",
                    endpoint,
                    data=data,
                    context=context
                )
                self.logger.info(
                    f"Task {task_id} completion reported successfully",
                    **context
                )
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    error = self.error_handler.handle_error(
                        e,
                        "api_backend_report_completion",
                        task_id=task_id,
                        **context
                    )
                    raise ProcessingError(
                        f"Failed to report completion after {max_retries} attempts",
                        ErrorType.PROCESSING_CRITICAL_FAILURE,
                        task_id=task_id
                    )
                await asyncio.sleep(2 ** attempt)
    
    async def report_task_progress(
        self,
        job_id: int,
        task_id: str,
        output_type: str,
        output_payload: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        CORRECTED: Uses primitives instead of TaskOutputRecord model.
        
        NOTE: In the new architecture, this method is NOT typically called via API.
        TaskOutputRecords are sent directly to Kafka topic 'prod_task_output_records'
        by the worker's Kafka producer, then consumed by Flink and written to DB.
        
        This endpoint exists for backward compatibility or emergency fallback,
        but in production EKS mode, the worker will bypass this and use Kafka directly.
        
        Endpoint: POST /api/v1/tasks/{task_id}/progress
        Body: {"job_id": X, "output_type": "...", "payload": {...}}
        
        Logic:
        1. Serialize data to JSON
        2. POST to backend
        3. Backend writes to task_output_records table (or forwards to Kafka)
        
        Error Handling:
        - NO retries - if fails, task must fail
        - Critical for pipeliner workflow
        """
        endpoint = f"/api/v1/tasks/{task_id}/progress"
        data = {
            "job_id": job_id,
            "output_type": output_type,
            "payload": output_payload
        }
        
        try:
            await self._make_request(
                "POST",
                endpoint,
                data=data,
                context=context
            )
            self.logger.info(
                f"TaskOutputRecord '{output_type}' saved via API",
                **context
            )
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "api_backend_report_progress",
                operation="write_task_output_record",
                **context
            )
            self.logger.critical(
                "FATAL: Failed to save TaskOutputRecord",
                error_id=error.error_id,
                **context
            )
            raise
    
    async def send_heartbeat(
        self,
        worker_id: str,
        task_id: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Sends heartbeat to backend API.
        
        Endpoint: POST /api/v1/workers/{worker_id}/heartbeat
        Body: {"task_id": "..."}
        
        Logic:
        1. POST to backend
        2. Backend renews lease_expiry in tasks table
        
        Error Handling:
        - Log warning but don't raise
        - Heartbeat is best-effort
        """
        endpoint = f"/api/v1/workers/{worker_id}/heartbeat"
        data = {"task_id": task_id}
        
        try:
            await self._make_request(
                "POST",
                endpoint,
                data=data,
                context=context
            )
            self.logger.debug(
                f"Heartbeat sent for task {task_id}",
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "api_backend_heartbeat",
                **context
            )
            self.logger.warning(
                "Failed to send heartbeat - lease may expire",
                **context
            )
    
    async def get_datasource_configuration(
        self,
        datasource_id: str,
        context: Dict[str, Any]
    ) -> DataSourceConfiguration:
        """
        Fetches datasource config from backend API.
        
        Endpoint: GET /api/v1/datasources/{datasource_id}/config
        
        Logic:
        1. GET from backend
        2. Backend queries database
        3. Returns JSON
        4. Parse into DataSourceConfiguration Pydantic model
        
        Error Handling:
        - 404: ConfigurationError (datasource not found)
        - Parse error: ConfigurationError (invalid config)
        """
        endpoint = f"/api/v1/datasources/{datasource_id}/config"
        
        try:
            response = await self._make_request(
                "GET",
                endpoint,
                context=context
            )
            
            # Parse into Pydantic model
            config = DataSourceConfiguration.parse_obj(response["configuration"])
            return config
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "api_backend_get_datasource_config",
                **context
            )
            raise
    
    async def get_connector_configuration(
        self,
        connector_type: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches connector config from backend API.
        
        Endpoint: GET /api/v1/connectors/{connector_type}/config
        """
        endpoint = f"/api/v1/connectors/{connector_type}/config"
        
        try:
            response = await self._make_request(
                "GET",
                endpoint,
                context=context
            )
            return response["configuration"]
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "api_backend_get_connector_config",
                **context
            )
            raise
    
    async def get_credential_reference(
        self,
        credential_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches credential reference from backend API.
        
        Endpoint: GET /api/v1/credentials/{credential_id}
        
        Note: Returns metadata only, not actual secret
        """
        endpoint = f"/api/v1/credentials/{credential_id}"
        
        try:
            response = await self._make_request(
                "GET",
                endpoint,
                context=context
            )
            return response["credential"]
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "api_backend_get_credential",
                **context
            )
            raise
    
    async def get_classifier_template(
        self,
        template_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches classifier template from backend API.
        
        Endpoint: GET /api/v1/templates/{template_id}
        """
        endpoint = f"/api/v1/templates/{template_id}"
        
        try:
            response = await self._make_request(
                "GET",
                endpoint,
                context=context
            )
            return response["template"]
            
        except Exception as e:
            error = self.error_handler.handle_error(
                e,
                "api_backend_get_template",
                **context
            )
            raise
    
    async def register_worker(
        self,
        worker_id: str,
        node_group: str,
        capabilities: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Registers worker via backend API.
        
        Endpoint: POST /api/v1/workers/register
        Body: {"worker_id": "...", "node_group": "...", "capabilities": {...}}
        """
        endpoint = "/api/v1/workers/register"
        data = {
            "worker_id": worker_id,
            "node_group": node_group,
            "capabilities": capabilities
        }
        
        try:
            await self._make_request(
                "POST",
                endpoint,
                data=data,
                context=context
            )
            self.logger.info(
                f"Worker {worker_id} registered successfully",
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "api_backend_register_worker",
                **context
            )
            self.logger.warning(
                "Worker registration failed but continuing",
                **context
            )
    
    async def deregister_worker(
        self,
        worker_id: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Deregisters worker via backend API.
        
        Endpoint: POST /api/v1/workers/deregister
        Body: {"worker_id": "..."}
        """
        endpoint = "/api/v1/workers/deregister"
        data = {"worker_id": worker_id}
        
        try:
            await self._make_request(
                "POST",
                endpoint,
                data=data,
                context=context
            )
            self.logger.info(
                f"Worker {worker_id} deregistered successfully",
                **context
            )
        except Exception as e:
            self.error_handler.handle_error(
                e,
                "api_backend_deregister_worker",
                **context
            )
