# src/core/interfaces/connector_interface.py
"""
Defines the abstract interface that all data source connectors must implement.
A Worker uses this interface to interact with different data sources in a
consistent way, abstracting the protocol-specific logic.
"""

from abc import ABC, abstractmethod
from typing import List, Iterator

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
    def enumerate_objects(self, work_packet: WorkPacket) -> Iterator[List[DiscoveredObject]]:
        """
        Performs a fast, streaming enumeration of objects from the source.

        - Input: The full, strongly-typed WorkPacket for a DISCOVERY_ENUMERATE task.
        - Intermediate Output: This method MUST be a generator. It yields batches
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
    def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]:
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
    def get_object_content(self, work_packet: WorkPacket) -> Iterator[dict]:
        """
        Retrieves the actual content of an object for classification.

        - Input: The full WorkPacket for a CLASSIFICATION task.
        - Intermediate Output: This method MUST be a generator. It yields a dictionary
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
    def enumerate_objects(self, work_packet: WorkPacket) -> Iterator[List[DiscoveredObject]]:
        pass
    
    @abstractmethod  
    def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]:
        pass
    
    @abstractmethod
    def get_object_content(self, work_packet: WorkPacket) -> Iterator[ContentComponent]:
        """
        Retrieves and extracts content components from files for classification.

        - Input: The full WorkPacket for a CLASSIFICATION task.
        - Intermediate Output: This method MUST be a generator. It yields ContentComponent
          objects representing extracted components (text, tables, images, archive members).
          This allows memory-safe streaming of complex file content.
        - Final Output: None. The generator is simply exhausted.
        - Logging Fields: Logs the object_id being processed and extraction methods used.
        - Error Handling: Should yield error components for failed extractions but continue processing.
        """
        pass


# src/core/interfaces/classification_engine_interface.py
"""
Defines the interface for the Classification Engine.
"""

from core.models.models import PIIFinding # Assuming your models.py is aliased

class IClassificationEngine(ABC):
    """
    Interface for the classification engine. The Worker calls this to
    perform the actual sensitive data analysis.
    """

    @abstractmethod
    def classify_content(self, object_id: str, content: any, work_packet: WorkPacket) -> List[PIIFinding]:
        """
        Analyzes a block of content for sensitive data based on the provided rules.

        - Input: The unique 'object_id' of the content being scanned, the 'content'
          itself (e.g., a string of text, a list of database rows), and the full
          WorkPacket, which contains the 'classifier_template_id' and other context.
        - Intermediate Output: None.
        - Final Output: Returns a list of strongly-typed PIIFinding objects.
        - Logging Fields: Logs the object_id and the classifier_template_id being used.
          For performance, it should NOT log the content itself.
        - Error Handling: Should not raise exceptions for content it cannot parse. Instead,
          it should log the error and return an empty list of findings.
        """
        pass


