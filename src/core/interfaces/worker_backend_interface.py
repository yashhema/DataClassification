# src/core/interfaces/worker_backend_interface.py
"""
Abstract interface for worker-backend communication.
Allows workers to operate in single-process or distributed mode.

CORRECTED: report_task_progress uses primitives instead of models (Addendum 3.1.1)
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from core.models.models import WorkPacket
from core.config.config_models import DataSourceConfiguration

class IWorkerBackend(ABC):
    """
    Interface defining all backend operations a worker needs.
    Implementations: DirectDBBackend (single-process), APIBackend (EKS)
    """
    
    @abstractmethod
    async def get_next_task(self, worker_id: str, node_group: str) -> Optional[WorkPacket]:
        """
        Fetches next available task for worker.
        
        Returns:
            WorkPacket if task available, None if queue empty
            
        Raises:
            NetworkError: If connection to backend fails
            RightsError: If worker not authorized for node_group
        """
        pass
    
    @abstractmethod
    async def report_task_completion(
        self, 
        task_id: str, 
        status: str, 
        result_payload: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Reports task completion to backend.
        
        Args:
            task_id: Hex string task identifier
            status: "COMPLETED" or "FAILED"
            result_payload: Final task results/error details
            context: Logging context (worker_id, job_id, etc.)
            
        Raises:
            NetworkError: If backend unreachable
            
        Note: Must handle retries internally, this is critical for workflow
        """
        pass
    
    @abstractmethod
    async def report_task_progress(
        self,
        job_id: int,
        task_id: str,
        output_type: str,
        output_payload: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Saves task progress for pipeliner processing.
        
        CRITICAL: This is NOT just status update - pipeliner depends on this
        to create next stage tasks. Must be reliable.
        
        Args:
            job_id: Job identifier
            task_id: Hex string task identifier
            output_type: e.g., "DISCOVERED_OBJECTS", "CLASSIFICATION_COMPLETE"
            output_payload: Stage-specific data dict
            context: Logging context
            
        Raises:
            ProcessingError: If write fails (should fail task)
            
        Note: Interface uses primitives, not models, for cross-boundary compatibility.
        In the new architecture, this sends data to Kafka topic 'prod_task_output_records',
        which is consumed by Flink and written to the database.
        """
        pass
    
    @abstractmethod
    async def send_heartbeat(
        self,
        worker_id: str,
        task_id: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Renews task lease to prevent reaper from requeueing.
        
        Args:
            worker_id: Worker identifier
            task_id: Hex string task identifier
            context: Logging context
            
        Note: Failures should log warning but not fail task
        """
        pass
    
    @abstractmethod
    async def get_datasource_configuration(
        self,
        datasource_id: str,
        context: Dict[str, Any]
    ) -> DataSourceConfiguration:
        """
        Fetches datasource connection config and scan profiles.
        
        Returns:
            DataSourceConfiguration (Pydantic model from config_models.py)
            
        Raises:
            ConfigurationError: If datasource not found or invalid config
        """
        pass
    
    @abstractmethod
    async def get_connector_configuration(
        self,
        connector_type: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches connector-specific config (SQL queries, settings).
        
        Args:
            connector_type: e.g., "sqlserver", "smb", "postgres"
            
        Returns:
            Dict containing query_sets, settings, etc.
            
        Raises:
            ConfigurationError: If connector config not found
        """
        pass
    
    @abstractmethod
    async def get_credential_reference(
        self,
        credential_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches credential reference (NOT the actual password/key).
        
        Returns metadata needed to retrieve actual credential from vault.
        Actual secret retrieval happens via CredentialManager.
        
        Returns:
            {
                "credential_id": str,
                "credential_type": "password|api_key|certificate",
                "vault_path": str,  # Path in vault system
                "metadata": {...}    # Additional hints
            }
            
        Raises:
            ConfigurationError: If credential not found
            RightsError: If worker not authorized for credential
        """
        pass
    
    @abstractmethod
    async def get_classifier_template(
        self,
        template_id: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetches full classifier template with all patterns, rules, exclusions.
        
        Returns:
            Complete template config as dict (used by EngineInterface)
            
        Raises:
            ConfigurationError: If template not found
        """
        pass
    
    @abstractmethod
    async def register_worker(
        self,
        worker_id: str,
        node_group: str,
        capabilities: Dict[str, Any],
        context: Dict[str, Any]
    ) -> None:
        """
        Registers worker in component registry on startup.
        
        Args:
            capabilities: {"version": "2.5.0", "supports": ["smb", "sqlserver"]}
            
        Raises:
            NetworkError: If registration fails
        """
        pass
    
    @abstractmethod
    async def deregister_worker(
        self,
        worker_id: str,
        context: Dict[str, Any]
    ) -> None:
        """
        Removes worker from registry on graceful shutdown.
        
        Note: Orchestrator will eventually remove stale workers via timeout
        """
        pass
