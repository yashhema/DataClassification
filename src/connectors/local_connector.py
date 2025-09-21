# src/connectors/local_connector.py
import os
import shutil
from typing import AsyncIterator, List, Dict, Any, Optional

from core.interfaces.worker_interfaces import IFileDataSourceConnector
from core.models.models import DiscoveredObject, RemediationResult, ContentComponent, ContentExtractionConfig, ObjectType, ObjectMetadata
from content_extraction.content_extractor import ContentExtractor
from core.utils.hash_utils import generate_object_key_hash
from datetime import datetime, timezone

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
        self.root_path = local_config.get("root_path")

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

    async def enumerate_objects(self, work_packet: Any) -> AsyncIterator[List[DiscoveredObject]]:
        """Enumerates files recursively and creates objects with correct hash keys."""
        self.logger.info(f"Starting local file enumeration at '{self.root_path}'...", datasource_id=self.datasource_id)
        
        batch = []
        batch_size = 100
        
        for dirpath, _, filenames in os.walk(self.root_path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    stat = os.stat(full_path)
                    object_type_str = "FILE"
                    obj_key_hash = generate_object_key_hash(
                        datasource_id=self.datasource_id,
                        object_path=full_path,
                        object_type=object_type_str
                    )
                    obj = DiscoveredObject(
                        object_key_hash=obj_key_hash,
                        datasource_id=self.datasource_id,
                        object_type=ObjectType.FILE,
                        object_path=full_path,
                        size_bytes=stat.st_size,
                        last_modified=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                        discovery_timestamp=datetime.now(timezone.utc)
                    )
                    batch.append(obj)
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                except OSError as e:
                    self.logger.warning(f"Could not access file {full_path}: {e}", file_path=full_path)
        
        if batch:
            yield batch




    async def get_object_content(self, work_packet: Any) -> AsyncIterator[ContentComponent]:
        """
        Passes the local file path directly to the ContentExtractor.
        No download is required.
        """
        object_ids = work_packet.payload.object_ids
        for file_path in object_ids:
            # The object_id *is* the file path for this connector
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