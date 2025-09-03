# src/core/interfaces/worker_interfaces.py
"""
Defines the abstract interface that all data source connectors must implement.
A Worker uses this interface to interact with different data sources in a
consistent way, abstracting the protocol-specific logic.

UPDATED: Converted to full async interface with AsyncIterator support.
"""

from abc import ABC, abstractmethod
from typing import List, AsyncIterator # UPDATED: Changed Iterator to AsyncIterator

# Import the strongly-typed Pydantic models
from core.models.discovered_object_schema import DiscoveredObject, ObjectMetadata
from core.models.models import WorkPacket, PIIFinding, ContentComponent 

class IDatabaseDataSourceConnector(ABC):
    """
    Interface for data source connectors. The Worker instantiates a concrete
    implementation of this class (e.g., SQLServerConnector, S3Connector)
    based on the datasource_id in the WorkPacket.
    """

    @abstractmethod
    async def enumerate_objects(self, work_packet: WorkPacket) -> AsyncIterator[List[DiscoveredObject]]: # UPDATED: async def and AsyncIterator
        """
        Performs a fast, streaming enumeration of objects from the source.

        - Input: The full, strongly-typed WorkPacket for a DISCOVERY_ENUMERATE task.
        - Intermediate Output: This method MUST be an async generator. It yields batches
          (lists) of DiscoveredObject records. The Worker is responsible for writing
          these batches to the staging table and reporting progress.
        - Final Output: After the generator is exhausted, it should return a dictionary
          containing a list of any new sub-task definitions (e.g., for sub-directories).
        - Logging Fields: The connector should log the paths/queries it is using, along
          with the common context fields from the work_packet.header.
        - Error Handling: Must raise specific, catchable exceptions from the errors.py
          hierarchy (e.g., NetworkError for connection issues, RightsError for
          permission denied).
        """
        pass

    @abstractmethod
    async def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]: # UPDATED: async def
        """
        Fetches rich, detailed metadata for a batch of objects.

        - Input: The full WorkPacket for a DISCOVERY_GET_DETAILS task.
        - Intermediate Output: None. This is treated as an atomic operation for the batch.
        - Final Output: Returns a list of fully populated ObjectMetadata objects. The
          Worker is responsible for writing these directly to the main ObjectMetadata table.
        - Logging Fields: Logs the count of object_ids being processed and which types
          of metadata are being fetched based on the config.
        - Error Handling: Must handle errors on a per-object basis, logging failures
          but continuing to process the rest of the batch if possible. Returns a partial
          list of successes.
        """
        pass

    @abstractmethod
    async def get_object_content(self, work_packet: WorkPacket) -> AsyncIterator[dict]: # UPDATED: async def and AsyncIterator
        """
        Retrieves the actual content of an object for classification.

        - Input: The full WorkPacket for a CLASSIFICATION task.
        - Intermediate Output: This method MUST be an async generator. It yields a dictionary
          for each object containing its 'object_id' and its 'content' (as a string or bytes).
          This allows the Worker to stream content for classification without loading
          everything into memory at once. For database tables, it would yield batches of rows.
        - Final Output: None. The generator is simply exhausted.
        - Logging Fields: Logs the object_id being read and any sampling method being used.
        - Error Handling: Must raise specific exceptions for corrupted or unreadable data.
        """
        pass


# Add new interface for file-based connectors
class IFileDataSourceConnector(ABC):
    """Interface for file-based datasource connectors (SMB, S3, Azure Blob, etc.)"""
    
    @abstractmethod
    async def enumerate_objects(self, work_packet: WorkPacket) -> AsyncIterator[List[DiscoveredObject]]: # UPDATED: async def and AsyncIterator
        pass
    
    @abstractmethod  
    async def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]: # UPDATED: async def
        pass
    
    @abstractmethod
    async def get_object_content(self, work_packet: WorkPacket) -> AsyncIterator[ContentComponent]: # UPDATED: async def and AsyncIterator
        """
        Retrieves and extracts content components from files for classification.

        - Input: The full WorkPacket for a CLASSIFICATION task.
        - Intermediate Output: This method MUST be an async generator. It yields ContentComponent
          objects representing extracted components (text, tables, images, archive members).
          This allows memory-safe streaming of complex file content.
        - Final Output: None. The generator is simply exhausted.
        - Logging Fields: Logs the object_id being processed and extraction methods used.
        - Error Handling: Should yield error components for failed extractions but continue processing.
        """
        pass