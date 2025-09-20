# src/core/interfaces/worker_interfaces.py
"""
Defines the abstract interfaces that all data source connectors must implement.
This includes interfaces for discovery, classification, and post-scan remediation actions.
"""

from abc import ABC, abstractmethod
from typing import List, AsyncIterator, Optional, Dict, Any, Tuple

# Import the strongly-typed Pydantic models
from core.models.models import DiscoveredObject, ObjectMetadata, RemediationResult
from core.models.models import WorkPacket, ContentComponent, TombstoneConfig


class IDatabaseDataSourceConnector(ABC):
    """
    Interface for data source connectors that interact with structured databases.
    """

    @abstractmethod
    async def enumerate_objects(self, work_packet: WorkPacket) -> AsyncIterator[List[DiscoveredObject]]:
        """Performs a fast, streaming enumeration of objects from the source."""

    @abstractmethod
    async def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]:
        """Fetches rich, detailed metadata for a batch of objects."""

    @abstractmethod
    async def get_object_content(self, work_packet: WorkPacket) -> AsyncIterator[dict]:
        """Retrieves the actual content of an object for classification."""

# NEW: Interface for post-scan remediation actions
class IRemediationConnector(ABC):
    """
    Interface for connectors that support post-scan remediation actions.
    These methods provide the "write" capabilities needed for a full
    data governance workflow.
    """

    @abstractmethod
    async def move_objects(self, 
                           source_paths: List[str], 
                           destination_directory: str,context: Dict[str, Any], 
                           tombstone_config: Optional[TombstoneConfig] = None
                           ) -> RemediationResult:
        """
        Moves a batch of objects. If tombstone_config is provided, it must create
        a tombstone file at the original location after a successful move.
        """

    @abstractmethod
    async def delete_objects(self, 
                           paths: List[str], context: Dict[str, Any],
                           tombstone_config: Optional[TombstoneConfig] = None,
                           ) -> RemediationResult:
        """
        Deletes a batch of objects. If tombstone_config is provided, it must create
        a tombstone file in place of the deleted object.
        """

    @abstractmethod
    async def tag_objects(self, 
                        objects_with_tags: List[Tuple[str, List[str]]],
                        context: Dict[str, Any]) -> RemediationResult:
        """
        Applies metadata tags to a batch of objects.
        Args:
            objects_with_tags: A list of tuples, where each tuple contains an
                               object_path and a list of tags to apply.
        """

    @abstractmethod
    async def apply_encryption(self, 
                              object_paths: List[str], 
                              encryption_key_id: str, 
                              context: Dict[str, Any]) -> RemediationResult:
        """
        Placeholder method to encrypt a batch of objects in-place.
        """

    @abstractmethod
    async def apply_mip_labels(self, 
                             objects_with_labels: List[Tuple[str, str]],
                             context: Dict[str, Any]) -> RemediationResult:
        """
        Placeholder method to apply a specific MIP label to a batch of objects.
        Args:
            objects_with_labels: A list of tuples, where each tuple contains an
                                 object_path and the specific MIP Label ID to apply.
        """

# UPDATED: IFileDataSourceConnector now inherits from IRemediationConnector
class IFileDataSourceConnector(IRemediationConnector):
    """Interface for file-based datasource connectors (SMB, S3, Azure Blob, etc.)"""
    
    @abstractmethod
    async def enumerate_objects(self, work_packet: WorkPacket) -> AsyncIterator[List[DiscoveredObject]]:
        pass
    
    @abstractmethod  
    async def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]:
        pass
    
    @abstractmethod
    async def get_object_content(self, work_packet: WorkPacket) -> AsyncIterator[ContentComponent]:
        """
        Retrieves and extracts content components from files for classification.
        """

