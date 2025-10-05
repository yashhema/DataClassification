# src/core/interfaces/worker_interfaces.py
"""
Defines the abstract interfaces that all data source connectors must implement.
This includes interfaces for discovery, classification, and post-scan remediation actions.
"""

from abc import ABC, abstractmethod
from typing import List, AsyncIterator, Optional, Dict, Any, Tuple

# Import the strongly-typed Pydantic models
from core.models.models import DiscoveredObject, ObjectMetadata, RemediationResult
from core.models.models import WorkPacket, ContentComponent, TombstoneConfig,BoundaryType, DiscoveryBatch,SystemProfile

"""
    Interface for data source connectors that interact with structured databases.

    --- DESIGN DOCUMENT: Resilient Enumeration Workflow ---

    This interface enforces a multi-level, fault-tolerant "fan-out" discovery
    process. The goal is to ensure that the enumeration of large, complex database
    servers is both scalable and fully recoverable.

    The enumeration process is broken down into a series of distinct, layered tasks,
    with each completed task serving as a permanent checkpoint in the database.

    THE LOGICAL FLOW:
    1.  Task 1 (Discover Databases): The first task connects to the server instance
        and discovers a list of all database names. It yields a single final
        DiscoveryBatch containing DiscoveredObjects of type DATABASE.

    2.  Pipeliner (Fan-Out): The Pipeliner receives this list and creates new,
        batched DISCOVERY_ENUMERATE tasks. Each new task is responsible for
        processing a batch of databases to find their schemas.

    3.  Task 2 (Discover Schemas): A worker receives a task with a list of database
        names. For each database, it discovers all schema names and yields a
        final DiscoveryBatch for that database, containing DiscoveredObjects of
        type SCHEMA.

    4.  Pipeliner (Fan-Out): The Pipeliner receives these lists of schemas and
        creates the final set of batched DISCOVERY_ENUMERATE tasks.

    5.  Task 3 (Discover Tables): A worker receives a task with a list of schema
        names. For each schema, it discovers all tables and yields a final
        DiscoveryBatch containing the final DiscoveredObjects of type DATABASE_TABLE.

    This layered approach provides critical fault tolerance. A failure at any
    stage is isolated to that specific task and boundary, and the system can
    resume from the last successful checkpoint.
"""

class IDatabaseDataSourceConnector(ABC):


    @abstractmethod
    def get_boundary_type(self) -> BoundaryType:
        """
        Returns the logical boundary type this connector enumerates
        (e.g., DATABASE, SCHEMA). This informs the Pipeliner how to create
        the next stage of enumeration tasks.
        """
        pass
    @abstractmethod
    async def enumerate_objects(self, work_packet: WorkPacket) -> AsyncIterator[DiscoveryBatch]:
        """
        Performs a single level of discovery based on the boundaries provided in the
        work packet (e.g., finds schemas within a database, or tables within a schema).

        IMPLEMENTATION REQUIREMENTS:
        - This method MUST NOT be recursive. It processes only one logical level at a time.
        - It must process each boundary from the work_packet.payload.paths list sequentially.
        - For each boundary, it must yield one or more DiscoveryBatch objects.
        - CRITICAL: After all objects for a single boundary have been yielded,
          it MUST yield a final DiscoveryBatch with `is_final_batch=True`. This signal
          is the transactional checkpoint for the Worker.
        - If processing a boundary fails, it MUST still yield a final batch with an
          empty list to prevent the Worker from stalling.
        """
        # The 'yield' statement is required to mark this as an async generator
        if False:
            yield

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
    """
    Interface for file-based datasource connectors (SMB, S3, Azure Blob, etc.)

    --- DESIGN DOCUMENT: Resilient Enumeration Workflow ---

    This interface enforces a fault-tolerant enumeration process where each
    directory is treated as a transactional unit of work.

    THE LOGICAL FLOW:
    1.  A DISCOVERY_ENUMERATE task's payload contains a list of directories to scan.
    2.  The Worker processes this list one directory at a time.
    3.  For each directory, the connector performs a single-level listing and streams
        back its findings (files and subdirectories) in DiscoveryBatch objects.
    4.  When the connector is finished with a directory, it sends a final
        DiscoveryBatch with `is_final_batch=True`.
    5.  The Worker receives this signal and commits the work for that directory
        to the database, creating a permanent checkpoint.
    6.  The Pipeliner sees the new subdirectories and creates new, batched
        DISCOVERY_ENUMERATE tasks for them.
    """
    @abstractmethod
    def get_boundary_type(self) -> BoundaryType:
        """
        Returns the logical boundary type this connector enumerates
        (e.g., DATABASE, SCHEMA). This informs the Pipeliner how to create
        the next stage of enumeration tasks.
        """
        pass
    
    @abstractmethod
    async def enumerate_objects(self, work_packet: WorkPacket) -> AsyncIterator[DiscoveryBatch]:
        """
        Performs a single-level enumeration for each directory in the work packet.

        IMPLEMENTATION REQUIREMENTS:
        - This method MUST NOT be recursive.
        - It must process each directory from the payload sequentially.
        - It MUST yield DiscoveryBatch objects, signaling completion of each
          directory with `is_final_batch=True`.
        """
        if False:
            yield

    
    @abstractmethod  
    async def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]:
        pass
    
    @abstractmethod
    async def get_object_content(self, work_packet: WorkPacket) -> AsyncIterator[ContentComponent]:
        """
        Retrieves and extracts content components from files for classification.
        """

class IComplianceConnector(ABC):
    """
    Interface for connectors that support advanced compliance and security scanning.
    Connectors can implement both IDatabaseDataSourceConnector AND
    IComplianceConnector to provide full functionality.
    """
    @abstractmethod
    async def get_system_profile(self) -> SystemProfile:
        """
        Returns a standardized SystemProfile object containing the target's
        full version, patch level, OS, deployment model, and other key
        identifiers.
        """
        pass