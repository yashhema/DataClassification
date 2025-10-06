# src/connectors/local_connector.py
import os
import shutil
from typing import AsyncIterator, List, Dict, Any, Optional
import time
from core.interfaces.worker_interfaces import IFileDataSourceConnector
from core.models.models import DiscoveredObject, RemediationResult, ContentComponent, ContentExtractionConfig, ObjectType, ObjectMetadata
from content_extraction.content_extractor import ContentExtractor
from core.utils.hash_utils import generate_object_key_hash
from datetime import datetime, timezone
from typing import AsyncIterator, List, Dict, Any, Optional,Tuple
from core.models.models import BoundaryType, DiscoveryBatch
class LocalConnector(IFileDataSourceConnector):
    """
    A connector for scanning a directory on the local file system.
    This is ideal for testing and development.
    """

    def __init__(self, datasource_id: str, local_config: Dict[str, Any], 
                 system_config: Any, logger: Any, error_handler: Any):
        self.datasource_id = datasource_id
        self.logger = logger
        self.error_handler = error_handler
        self.root_path = local_config.get("connection", {}).get("root_path")

        if not self.root_path or not os.path.isdir(self.root_path):
            raise ValueError(f"Root path '{self.root_path}' is not a valid directory.")

        # 1. Parse content extraction settings from the data source config.
        extraction_dict = local_config.get('content_extraction', {})
        self.extraction_config = ContentExtractionConfig.from_dict(extraction_dict)
        
        # 2. CRITICAL: For a local connector, we work on original files,
        #    so we must NOT clean up/delete them.
        self.extraction_config.features.cleanup_temp_files = False
        
        # 3. Initialize a new ContentExtractor with the specific config for this connector.
        self.content_extractor = ContentExtractor(
            extraction_config=self.extraction_config,
            system_config=system_config,
            logger=self.logger,
            error_handler=self.error_handler
        )
    def get_boundary_type(self) -> BoundaryType:
        """For file systems, the enumeration boundary is always a directory."""
        return BoundaryType.DIRECTORY


    async def enumerate_objects(self, work_packet: Any) -> AsyncIterator[DiscoveryBatch]:
        """
        Performs a single-level enumeration for a batch of directories,
        streaming results back in the DiscoveryBatch format.
        """
        paths_to_scan = work_packet.payload.paths or [self.root_path]
        
        for mdirectory_path in paths_to_scan:
            directory_path = os.path.normpath(mdirectory_path)
            boundary_id = generate_object_key_hash(
                self.datasource_id, 
                directory_path, 
                self.get_boundary_type().value
            )
            boundary_path = f"{self.datasource_id}:{directory_path}:{self.get_boundary_type().value}"
            batch = []
            batch_size = 100
            last_yield_time = time.time()
            timeout_seconds = 5
            
            try:
                # Use os.scandir for a more efficient single-level listing
                with os.scandir(directory_path) as it:
                    for entry in it:
                        # Check timeout before processing to avoid hangs in slow directories
                        current_time = time.time()
                        if batch and (current_time - last_yield_time) > timeout_seconds:
                            yield DiscoveryBatch(
                                boundary_id=boundary_id,boundary_path=boundary_path, 
                                is_final_batch=False, 
                                discovered_objects=batch
                            )
                            batch = []
                            last_yield_time = current_time
                        
                        try:
                            object_type = ObjectType.DIRECTORY if entry.is_dir() else ObjectType.FILE
                            
                            obj_key_hash = generate_object_key_hash(
                                datasource_id=self.datasource_id,
                                object_path=entry.path,
                                object_type=object_type.value
                            )
                            stat = entry.stat()
                            obj = DiscoveredObject(
                                object_key_hash=obj_key_hash,
                                datasource_id=self.datasource_id,
                                object_type=object_type,
                                object_path=entry.path,
                                size_bytes=stat.st_size,
                                last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                                discovery_timestamp=datetime.now(timezone.utc)
                            )
                            batch.append(obj)
                            
                            # Yield when batch is full
                            if len(batch) >= batch_size:
                                yield DiscoveryBatch(
                                    boundary_id=boundary_id,boundary_path=boundary_path, 
                                    is_final_batch=False, 
                                    discovered_objects=batch
                                )
                                batch = []
                                last_yield_time = time.time()
                                
                        except OSError as e:
                            self.logger.warning(
                                f"Could not access entry {entry.path}: {e}", 
                                file_path=entry.path
                            )
                            
            except OSError as e:
                self.logger.error(
                    f"Could not scan directory {directory_path}: {e}", 
                    directory_path=directory_path
                )
            
            # Always yield final batch for this directory (even if empty or after error)
            yield DiscoveryBatch(
                boundary_id=boundary_id,boundary_path=boundary_path, 
                is_final_batch=True, 
                discovered_objects=batch
            )

    async def get_object_content(self, work_packet: Any) -> AsyncIterator[ContentComponent]:
            """
            Passes the local file path directly to the ContentExtractor.
            """
            # Access the correct attribute: 'discovered_objects'
            discovered_objects = work_packet.payload.discovered_objects

            # Loop through the list of DiscoveredObject models
            for discovered_obj in discovered_objects:

                if discovered_obj.object_type != ObjectType.FILE:
                    self.logger.info(
                        f"Skipping content extraction for non-file object: {discovered_obj.object_path}",
                        object_path=discovered_obj.object_path,
                        object_type=discovered_obj.object_type.value
                    )
                    continue
                
                # Extract the 'object_path' from each model


                file_path = discovered_obj.object_path

                async for component in self.content_extractor.extract_from_file(
                    file_path=file_path,
                    object_id=file_path,
                    job_id=work_packet.header.job_id,
                    task_id=work_packet.header.task_id,
                    datasource_id=self.datasource_id
                ):
                    yield component

    # --- Remediation Methods ---

    async def delete_objects(self, paths: List[str], context: Dict[str, Any], tombstone_config: Optional[Any] = None) -> RemediationResult:
        succeeded, failed = [], []
        for path in paths:
            try:
                os.remove(path)
                succeeded.append(path)
                # In a real implementation, you would create a tombstone file here if configured
            except OSError as e:
                failed.append({"path": path, "error_message": str(e)})
        return RemediationResult(succeeded_paths=succeeded, failed_paths=failed, success_count=len(succeeded), failure_count=len(failed))

    async def move_objects(self, source_paths: List[str], destination_directory: str, context: Dict[str, Any], tombstone_config: Optional[Any] = None) -> RemediationResult:
        succeeded, failed = [], []
        os.makedirs(destination_directory, exist_ok=True)
        for path in source_paths:
            try:
                filename = os.path.basename(path)
                shutil.move(path, os.path.join(destination_directory, filename))
                succeeded.append(path)
            except (OSError, shutil.Error) as e:
                failed.append({"path": path, "error_message": str(e)})
        return RemediationResult(succeeded_paths=succeeded, failed_paths=failed, success_count=len(succeeded), failure_count=len(failed))
        
    async def get_object_details(self, work_packet: Any) -> List[ObjectMetadata]:
        """
        Fetches detailed metadata. Not implemented for LocalConnector.
        """
        self.logger.info("get_object_details is not implemented for LocalConnector.")
        pass

    async def tag_objects(self, objects_with_tags: List[Tuple[str, List[str]]], context: Dict[str, Any]) -> RemediationResult:
        """
        Applies metadata tags. Not implemented for LocalConnector.
        """
        self.logger.info("tag_objects is not implemented for LocalConnector.")
        pass

    async def apply_encryption(self, object_paths: List[str], encryption_key_id: str, context: Dict[str, Any]) -> RemediationResult:
        """
        Encrypts objects. Not implemented for LocalConnector.
        """
        self.logger.info("apply_encryption is not implemented for LocalConnector.")
        pass

    async def apply_mip_labels(self, objects_with_labels: List[Tuple[str, str]], context: Dict[str, Any]) -> RemediationResult:
        """
        Applies MIP labels. Not implemented for LocalConnector.
        """
        self.logger.info("apply_mip_labels is not implemented for LocalConnector.")
        pass