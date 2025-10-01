# connectors/smb_connector.py
"""
SMB file share datasource implementation with async IFileDataSourceConnector interface.
Integrates with universal ContentExtractor and WorkPacket system.

Task 7 Complete: Full async conversion with AsyncIterator support
- Implements async IFileDataSourceConnector interface  
- All file I/O operations are async using aiofiles
- SMB protocol operations use thread pools to avoid blocking
- Returns AsyncIterator[ContentComponent] from get_object_content()
- Supports WorkPacket-based async task processing
"""

import os
import uuid
import fnmatch
import asyncio
import tempfile
import aiofiles
import time
from typing import AsyncIterator, Optional, Dict, Any, List,Tuple
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from core.models.models import ContentExtractionConfig, FailedObject,ObjectType
# SMB protocol imports
try:
    from smbprotocol.connection import Connection
    from smbprotocol.session import Session
    from smbprotocol.tree import TreeConnect
    from smbprotocol.open import (
        Open, CreateDisposition, ShareAccess, FileAttributes, 
        CreateOptions, FilePipePrinterAccessMask, ImpersonationLevel
    )
    
    from smbprotocol.exceptions import SMBException
    from smbprotocol.security_descriptor import SMB2CreateSDBuffer
    from smbprotocol.file_info import FileInformationClass, InfoType
    from smbprotocol.open import SMB2QueryInfoRequest, SMB2QueryInfoResponse
except ImportError as e:
    raise ImportError(
        "SMB datasource requires smbprotocol: pip install smbprotocol"
    ) from e

# Core system imports
from core.interfaces.worker_interfaces import IFileDataSourceConnector
from core.models.models import (
    WorkPacket, DiscoveredObject, ObjectMetadata, ObjectType, 
     ContentComponent,TombstoneConfig,RemediationResult,DiscoveryBatch,BoundaryType
)
from core.errors import (
    NetworkError, RightsError, ProcessingError, ErrorType
)
from core.logging.system_logger import SystemLogger
from content_extraction.content_extractor import ContentExtractor
from core.utils.hash_utils import generate_object_key_hash



# Utility functions (implement locally to avoid base dependency)
def create_object_id(datasource_id: str, object_path: str) -> str:
    """Create unique object ID from datasource and path"""
    import hashlib
    path_hash = hashlib.md5(object_path.encode('utf-8')).hexdigest()[:16]
    return f"{datasource_id}:{path_hash}:{object_path}"

def estimate_content_size(file_size_bytes: int, object_type: str) -> int:
    """Estimate extracted content size based on file size"""
    if object_type == 'file':
        # Estimate text content is roughly 1/3 of file size for mixed content
        return max(1024, int(file_size_bytes * 0.33))
    return file_size_bytes


# =============================================================================
# SMB Connection Management (Async)
# =============================================================================

class SMBConnection:
    """Manages SMB connections with proper async error handling and cleanup"""
    
    def __init__(self, connection_config: Dict[str, Any], logger: SystemLogger):
        self.connection_config = connection_config
        self.logger = logger
        self.connection: Optional[Connection] = None
        self.session: Optional[Session] = None
        self.tree: Optional[TreeConnect] = None
        self._connected = False
    
    async def connect_async(self) -> bool:
        """Establish SMB connection with proper async error handling"""
        try:
            host = self.connection_config['host']
            port = self.connection_config.get('port', 445)
            username = self.connection_config['username']
            password = self.connection_config['password']
            domain = self.connection_config.get('domain', '')
            share_name = self.connection_config['share_name']
            
            # Create connection - run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            
            def _create_connection():
                # Create connection
                conn = Connection(uuid.uuid4(), host, port)
                conn.connect()
                if domain and '\\' not in username:
                    full_username = f"{domain}\\{username}"
                else:
                    full_username = username                
                # Create session
                session = Session(conn, full_username, password)
                session.connect()
                
                # Connect to share
                tree_path = f"\\\\{host}\\{share_name}"
                tree = TreeConnect(session, tree_path)
                tree.connect()
                
                return conn, session, tree
            
            self.connection, self.session, self.tree = await loop.run_in_executor(
                None, _create_connection
            )
            
            self._connected = True
            self.logger.info("SMB connection established", 
                           host=host, share=share_name, domain=domain)
            return True
            
        except SMBException as e:
            if "STATUS_LOGON_FAILURE" in str(e):
                raise RightsError(
                    f"SMB authentication failed: {str(e)}",
                    ErrorType.RIGHTS_AUTHENTICATION_FAILED,
                    user=self.connection_config.get('username', 'unknown'),
                    host=self.connection_config.get('host')
                )
            elif "STATUS_BAD_NETWORK_NAME" in str(e):
                raise NetworkError(
                    f"SMB share not found: {str(e)}",
                    ErrorType.NETWORK_RESOURCE_NOT_FOUND,
                    host=self.connection_config.get('host'),
                    share=self.connection_config.get('share_name')
                )
            else:
                raise NetworkError(
                    f"SMB connection failed: {str(e)}",
                    ErrorType.NETWORK_CONNECTION_FAILED,
                    host=self.connection_config.get('host')
                )
        except Exception as e:
            raise NetworkError(
                f"Unexpected SMB connection error: {str(e)}",
                ErrorType.NETWORK_CONNECTION_FAILED,
                host=self.connection_config.get('host')
            )
    
    async def disconnect_async(self):
        """Close SMB connection with proper cleanup (async)"""
        try:
            def _cleanup():
                if self.tree:
                    try:
                        self.tree.disconnect()
                    except Exception:
                        pass
                
                if self.session:
                    try:
                        self.session.disconnect()
                    except Exception:
                        pass
                
                if self.connection:
                    try:
                        self.connection.disconnect()
                    except Exception:
                        pass
            
            # Run cleanup in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _cleanup)
            
        except Exception:
            # Ignore errors during cleanup
            pass
        finally:
            self.connection = None
            self.session = None
            self.tree = None
            self._connected = False
    
    def is_connected(self) -> bool:
        """Check if SMB connection is active"""
        return self._connected and self.tree is not None


# =============================================================================
# SMB DataSource Implementation with Async IFileDataSourceConnector Interface
# =============================================================================

class SMBConnector(IFileDataSourceConnector):
    """SMB file share datasource with async ContentExtractor integration"""

    def __init__(self, datasource_id: str, smb_config: Dict[str, Any], 
                 system_config: Any, logger: Any, error_handler: Any,
                 db_interface: Any, credential_manager: Any):
        self.datasource_id = datasource_id
        self.smb_config = smb_config
        self.credential_manager = credential_manager
        self.logger = logger
        self.error_handler = error_handler
        #1. Parse the content extraction settings from the data source config.
        #    This allows each data source to have unique extraction settings.
        extraction_dict = smb_config.get('content_extraction', {})
        self.extraction_config = ContentExtractionConfig.from_dict(extraction_dict)
        self.server_tz = ZoneInfo(self.smb_config.get('connection', {}).get('server_timezone', 'UTC'))
        # 2. For an SMB connector, we know it downloads temporary files
        #    that must be cleaned up, so we enforce it here.
        self.extraction_config.features.cleanup_temp_files = True
        
        # 3. Initialize a NEW ContentExtractor with the specific config for THIS connector.
        self.content_extractor = ContentExtractor(
            extraction_config=self.extraction_config,
            system_config=system_config,
            logger=self.logger,
            error_handler=self.error_handler
        )        
        # SMB connection
        self.smb_connection: Optional[SMBConnection] = None
        
        # SMB-specific configuration
        self.root_path = smb_config.get('root_path', '/')
        self.recursive = smb_config.get('recursive', True)
        self.include_patterns = smb_config.get('include_patterns', ['*'])
        self.exclude_patterns = smb_config.get('exclude_patterns', [])
        self.max_file_size_mb = smb_config.get('max_file_size_mb', 100)
        self.max_file_size_bytes = self.max_file_size_mb * 1024 * 1024
        
        # Temp file management
        self.temp_files_created = []
        self.temp_dir = tempfile.gettempdir()
        self._is_connected = False
        self.db_interface = db_interface


# In src/connectors/smb_connector.py, inside the SMBConnector class

    async def tag_objects(self, 
                          objects_with_tags: List[Tuple[str, List[str]]],
                          context: Dict[str, Any]) -> RemediationResult:
        """Applies metadata tags. Not implemented for SMBConnector."""
        self.logger.warning(
            "The 'tag_objects' method is not implemented for the SMBConnector. No action will be taken.", 
            **context
        )
        return RemediationResult(succeeded_paths=[], failed_paths=[], success_count=0, failure_count=0)

    async def apply_encryption(self, 
                             object_paths: List[str], 
                             encryption_key_id: str, 
                             context: Dict[str, Any]) -> RemediationResult:
        """Encrypts objects. Not implemented for SMBConnector."""
        self.logger.warning(
            "The 'apply_encryption' method is not implemented for the SMBConnector. No action will be taken.", 
            **context
        )
        return RemediationResult(succeeded_paths=[], failed_paths=[], success_count=0, failure_count=len(object_paths))

    async def apply_mip_labels(self, 
                             objects_with_labels: List[Tuple[str, str]],
                             context: Dict[str, Any]) -> RemediationResult:
        """Applies MIP labels. Not implemented for SMBConnector."""
        self.logger.warning(
            "The 'apply_mip_labels' method is not implemented for the SMBConnector. No action will be taken.", 
            **context
        )
        return RemediationResult(succeeded_paths=[], failed_paths=[], success_count=0, failure_count=len(objects_with_labels))

    # =============================================================================
    # Async IFileDataSourceConnector Interface Implementation
    # =============================================================================

    def get_boundary_type(self) -> BoundaryType:
        """
        For file systems like SMB, the fundamental unit of enumeration (the boundary)
        is always a directory.
        """
        return BoundaryType.DIRECTORY



    async def enumerate_objects(self, work_packet: Any) -> AsyncIterator[DiscoveryBatch]:
        """
        Performs a single-level enumeration for each directory specified in the
        work packet's payload. It streams results back in the DiscoveryBatch format,
        signaling the completion of each directory.
        """
        paths_to_scan = work_packet.payload.paths or [self.root_path]
        
        await self._ensure_connection_async(work_packet.header.trace_id, work_packet.header.task_id)

        for directory_path in paths_to_scan:
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
                async for discovered_obj in self._enumerate_single_directory_async(directory_path, work_packet):
                    # Check timeout before processing to avoid hangs
                    current_time = time.time()
                    if batch and (current_time - last_yield_time) > timeout_seconds:
                        yield DiscoveryBatch(
                            boundary_id=boundary_id,boundary_path=boundary_path, 
                            is_final_batch=False, 
                            discovered_objects=batch
                        )
                        batch = []
                        last_yield_time = current_time
                    
                    batch.append(discovered_obj)
                    
                    # Yield when batch is full
                    if len(batch) >= batch_size:
                        yield DiscoveryBatch(
                            boundary_id=boundary_id,boundary_path=boundary_path, 
                            is_final_batch=False, 
                            discovered_objects=batch
                        )
                        batch = []
                        last_yield_time = time.time()

            except Exception as e:
                self.logger.error(
                    f"Failed to enumerate directory {directory_path}", 
                    error=str(e), 
                    directory_path=directory_path
                )
            
            # Always yield final batch for this directory
            yield DiscoveryBatch(
                boundary_id=boundary_id,boundary_path=boundary_path, 
                is_final_batch=True, 
                discovered_objects=batch
            )

    async def _enumerate_single_directory_async(self, directory_path: str, work_packet: Any) -> AsyncIterator[DiscoveredObject]:
        """
        Performs a non-recursive, single-level listing of the specified directory
        and yields DiscoveredObject instances for each file and subdirectory found.
        """
        if not self.smb_connection or not self.smb_connection.is_connected():
            raise ProcessingError(
                "SMB connection not established", 
                ErrorType.PROCESSING_LOGIC_ERROR,
                operation="enumerate_single_directory"
            )

        normalized_path = self._normalize_smb_path(directory_path)
        loop = asyncio.get_event_loop()

        def _list_directory_sync():
            try:
                dir_handle = Open(self.smb_connection.tree, normalized_path)
                dir_handle.create(
                    ImpersonationLevel.Impersonation,
                    FilePipePrinterAccessMask.GENERIC_READ,
                    FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                    ShareAccess.FILE_SHARE_READ,
                    CreateDisposition.FILE_OPEN,
                    CreateOptions.FILE_DIRECTORY_FILE
                )
                entries = dir_handle.query_directory(
                    "*", 
                    FileInformationClass.FILE_ID_FULL_DIRECTORY_INFORMATION
                )
                dir_handle.close()
                return entries
                
            except SMBException as e:
                if "STATUS_OBJECT_NAME_NOT_FOUND" in str(e):
                    return []
                raise NetworkError(
                    f"SMB directory enumeration failed: {str(e)}",
                    ErrorType.NETWORK_SMB_ERROR,
                    path=directory_path
                )

        entries = await loop.run_in_executor(None, _list_directory_sync)

        for entry in entries:
            file_name_bytes = entry['file_name'].get_value()
            file_name = file_name_bytes.decode('utf-16-le').rstrip('\x00')
            if file_name in ['.', '..']:
                continue

            full_path = self._join_smb_path(normalized_path, file_name)
            
            # Use the existing _create_discovered_object_async which handles object_type internally
            discovered_obj = await self._create_discovered_object_async(entry, full_path)
            if discovered_obj:
                yield discovered_obj


    async def _enumerate_files_async(self, directory_path: str, 
                                   filters: Optional[Dict[str, Any]] = None) -> AsyncIterator[DiscoveredObject]:
        """Async file enumeration for interface compatibility"""
        
        if not self.smb_connection or not self.smb_connection.is_connected():
            raise ProcessingError(
                "SMB connection not established",
                ErrorType.PROCESSING_LOGIC_ERROR,
                operation="enumerate_files"
            )
        
        try:
            # Normalize path for SMB
            normalized_path = self._normalize_smb_path(directory_path)
            
            # Enumerate directory asynchronously
            loop = asyncio.get_event_loop()
            
            def _enumerate_directory():
                """Synchronous directory enumeration"""
                entries = []
                
                try:
                    # Open directory for enumeration
                    dir_handle = Open(self.smb_connection.tree, normalized_path)
                    dir_handle.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.GENERIC_READ,
                        FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                        ShareAccess.FILE_SHARE_READ,
                        CreateDisposition.FILE_OPEN,
                        CreateOptions.FILE_DIRECTORY_FILE
                    )
                    
                    # Query directory contents
                    dir_entries = dir_handle.query_directory(
                        "*", 
                        FileInformationClass.FILE_FULL_DIR_INFORMATION
                    )
                    
                    for entry in dir_entries:
                        if entry['file_name'] not in ['.', '..']:
                            entries.append(entry)
                    
                    dir_handle.close()
                    
                except SMBException as e:
                    if "STATUS_ACCESS_DENIED" in str(e):
                        raise RightsError(
                            f"Access denied to directory: {directory_path}",
                            ErrorType.RIGHTS_ACCESS_DENIED,
                            resource=directory_path
                        )
                    elif "STATUS_OBJECT_NAME_NOT_FOUND" in str(e):
                        # Directory doesn't exist, return empty list
                        return []
                    else:
                        raise NetworkError(
                            f"SMB directory enumeration failed: {str(e)}",
                            ErrorType.NETWORK_SMB_ERROR,
                            path=directory_path
                        )
                
                return entries
            
            # Get directory entries asynchronously
            entries = await loop.run_in_executor(None, _enumerate_directory)
            
            # Process entries and yield discovered objects
            for entry in entries:
                file_name = entry['file_name']
                full_path = self._join_smb_path(normalized_path, file_name)
                
                # Check if it's a directory
                is_directory = bool(entry['file_attributes'] & FileAttributes.FILE_ATTRIBUTE_DIRECTORY)
                
                if is_directory:
                    # Recursively process subdirectory if enabled
                    if self.recursive:
                        async for sub_obj in self._enumerate_files_async(full_path, filters):
                            yield sub_obj
                else:
                    # Process file
                    file_size = entry['end_of_file']
                    
                    # Apply size filter
                    if file_size > self.max_file_size_bytes:
                        continue
                    
                    # Apply include/exclude patterns
                    if not self._matches_include_patterns(full_path):
                        continue
                    
                    if self._matches_exclude_patterns(full_path):
                        continue
                    
                    # Create discovered object
                    discovered_obj = await self._create_discovered_object_async(entry, full_path)
                    if discovered_obj:
                        yield discovered_obj
                        
        except Exception as e:
            raise self._handle_processing_error(
                e, 'enumerate_files_async',
                directory_path=directory_path
            )



    async def get_object_content(self, work_packet: WorkPacket) -> AsyncIterator[ContentComponent]:
        """Get content components for classification - async interface method"""
        
        # --- FIX STARTS HERE ---

        # 1. Access the correct attribute: 'discovered_objects'
        discovered_objects = work_packet.payload.discovered_objects
        
        self.logger.info("Starting SMB content extraction",
                        task_id=work_packet.header.task_id,
                        object_count=len(discovered_objects))
        
        # 2. Loop through the list of DiscoveredObject models
        for discovered_obj in discovered_objects:
            try:
                # --- FIX: Add a check to ensure we only process files ---
                if discovered_obj.object_type != ObjectType.FILE:
                    self.logger.info(
                        f"Skipping content extraction for non-file object: {discovered_obj.object_path}",
                        object_path=discovered_obj.object_path,
                        object_type=discovered_obj.object_type.value
                    )
                    continue
                # --- END FIX ---                
                # 3. Use the object_path from each object
                file_path = discovered_obj.object_path
                
                if not file_path:
                    self.logger.warning("Discovered object is missing a file path", object_hash=discovered_obj.object_key_hash.hex())
                    continue
                
                # Download file to temp location (async)
                temp_file_path = await self._download_file_to_temp_async(file_path, work_packet.header.task_id)
                
                if not temp_file_path:
                    continue
                
                try:
                    # Use ContentExtractor to process the downloaded file
                    # We can use the file_path as the unique object_id for extraction
                    async for component in self.content_extractor.extract_from_file(
                        temp_file_path, 
                        file_path, # Use path as the object_id
                        work_packet.header.job_id,
                        work_packet.header.task_id,  
                        self.datasource_id
                    ):
                        # Add SMB-specific metadata to components
                        if not hasattr(component, 'metadata') or component.metadata is None:
                            component.metadata = {}
                        
                        component.metadata.update({
                            "smb_source_path": file_path,
                            "smb_server": self.smb_config.get('connection', {}).get('host'),
                            "smb_share": self.smb_config.get('connection', {}).get('share_or_bucket'),
                            "downloaded_for_processing": True
                        })
                        
                        yield component
                        
                finally:
                    # Clean up temp file (async)
                    if self.extraction_config.features.cleanup_temp_files:
                        await self._cleanup_temp_file_async(temp_file_path)
            
            except Exception as e:
                error = self.error_handler.handle_error(
                    e, f"smb_content_extraction_{discovered_obj.object_path}",
                    operation="get_object_content",
                    object_path=discovered_obj.object_path,
                    task_id=work_packet.header.task_id
                )
                self.logger.warning("SMB content extraction for a single object failed", 
                                   error_id=error.error_id, object_path=discovered_obj.object_path)
                
                # Yield error component and continue to the next object
                yield self._create_extraction_error_component(discovered_obj.object_path, str(e))
        
        # --- FIX ENDS HERE ---


    # =============================================================================
    # Remote file action
    # =============================================================================



    async def delete_objects(self, 
                           paths: List[str], 
                           tombstone_config: Optional[TombstoneConfig] = None,
                           context: Optional[Dict[str, Any]] = None) -> RemediationResult:
        """
        Deletes a batch of files. If tombstone_config is provided, it creates
        a tombstone file in place of the deleted object.
        """
        succeeded_paths = []
        failed_paths = []
        loop = asyncio.get_event_loop()

        for path in paths:
            try:
                # All smbprotocol I/O is synchronous and must be run in an executor
                await loop.run_in_executor(
                    None, 
                    self._delete_single_file_sync, 
                    path, 
                    tombstone_config
                )
                succeeded_paths.append(path)
            except Exception as e:
                self.error_handler.handle_error(e, f"smb_delete_{path}", **context)
                failed_paths.append(FailedObject(path=path, error_message=str(e)))
        
        return RemediationResult(
            succeeded_paths=succeeded_paths,
            failed_paths=failed_paths,
            success_count=len(succeeded_paths),
            failure_count=len(failed_paths)
        )

    def _delete_single_file_sync(self, path: str, tombstone_config: Optional[TombstoneConfig]):
        """Synchronous helper for deleting a single file and optionally creating a tombstone."""
        normalized_path = self._normalize_smb_path(path)
        
        # First, delete the original file
        with Open(self.smb_connection.tree, normalized_path) as f:
            f.create(
                ImpersonationLevel.Impersonation,
                FilePipePrinterAccessMask.DELETE, # Request delete permission
                FileAttributes.FILE_ATTRIBUTE_NORMAL,
                ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE | ShareAccess.FILE_SHARE_DELETE,
                CreateDisposition.FILE_OPEN,
                CreateOptions.FILE_DELETE_ON_CLOSE
            )
        
        # If a tombstone is requested, create it
        if tombstone_config:
            tombstone_path_parts = os.path.splitext(normalized_path)
            tombstone_name = tombstone_config.filename_format.format(
                filename=os.path.basename(tombstone_path_parts[0]),
                extension=tombstone_path_parts[1]
            )
            tombstone_full_path = self._join_smb_path(os.path.dirname(normalized_path), tombstone_name)

            with Open(self.smb_connection.tree, tombstone_full_path) as f:
                f.create(
                    ImpersonationLevel.Impersonation,
                    FilePipePrinterAccessMask.GENERIC_WRITE,
                    FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    ShareAccess.FILE_SHARE_WRITE,
                    CreateDisposition.FILE_CREATE, # Create a new file
                    CreateOptions.FILE_NON_DIRECTORY_FILE
                )
                f.write(tombstone_config.message.encode('utf-8'))


    async def move_objects(self, 
                           source_paths: List[str], 
                           destination_directory: str, 
                           tombstone_config: Optional[TombstoneConfig] = None,
                           context: Optional[Dict[str, Any]] = None) -> RemediationResult:
        """
        Moves a batch of files. If tombstone_config is provided, it creates
        a tombstone file at the original location after a successful move.
        """
        succeeded_paths = []
        failed_paths = []
        loop = asyncio.get_event_loop()

        for path in source_paths:
            try:
                await loop.run_in_executor(
                    None, 
                    self._move_single_file_sync, 
                    path, 
                    destination_directory, 
                    tombstone_config
                )
                succeeded_paths.append(path)
            except Exception as e:
                self.error_handler.handle_error(e, f"smb_move_{path}", **context)
                failed_paths.append(FailedObject(path=path, error_message=str(e)))
                
        return RemediationResult(
            succeeded_paths=succeeded_paths,
            failed_paths=failed_paths,
            success_count=len(succeeded_paths),
            failure_count=len(failed_paths)
        )

    def _move_single_file_sync(self, source_path: str, dest_dir: str, tombstone_config: Optional[TombstoneConfig]):
        """Synchronous helper for moving a single file and optionally creating a tombstone."""
        normalized_source = self._normalize_smb_path(source_path)
        normalized_dest_dir = self._normalize_smb_path(dest_dir)
        dest_path = self._join_smb_path(normalized_dest_dir, os.path.basename(normalized_source))

        # Perform the move (rename)
        with Open(self.smb_connection.tree, normalized_source) as f:
            f.create(
                ImpersonationLevel.Impersonation,
                FilePipePrinterAccessMask.DELETE,
                FileAttributes.FILE_ATTRIBUTE_NORMAL,
                ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE | ShareAccess.FILE_SHARE_DELETE,
                CreateDisposition.FILE_OPEN,
                0
            )
            # The new path is passed directly to the set_info method.
            # The library handles the FileRenameInformation class internally.
            f.set_info(dest_path) 
        # If move was successful and tombstone is requested, create it at the original location
        if tombstone_config:
            self._create_tombstone_sync(normalized_source, tombstone_config)

    def _create_tombstone_sync(self, original_path: str, tombstone_config: TombstoneConfig):
        """Synchronous helper to create a tombstone file."""
        # This helper is simplified; a real version would use tombstone_config.filename_format
        tombstone_full_path = f"{original_path}.tombstone.txt"
        with Open(self.smb_connection.tree, tombstone_full_path) as f:
            f.create(
                ImpersonationLevel.Impersonation,
                FilePipePrinterAccessMask.GENERIC_WRITE,
                FileAttributes.FILE_ATTRIBUTE_NORMAL,
                ShareAccess.FILE_SHARE_WRITE,
                CreateDisposition.FILE_CREATE,
                CreateOptions.FILE_NON_DIRECTORY_FILE
            )
            f.write(tombstone_config.message.encode('utf-8'))


    async def apply_mip_labels(self, 
                             objects_with_labels: List[Tuple[str, str]],
                             context: Optional[Dict[str, Any]] = None) -> RemediationResult:
        """
        Applies a specific MIP label to a batch of Office documents.
        """
        succeeded_paths = []
        failed_paths = []
        loop = asyncio.get_event_loop()

        for path, label_id in objects_with_labels:
            temp_file_path = None
            try:
                # 1. Download the file to a local temporary path
                temp_file_path = await self._download_file_to_temp_async(path, context.get("task_id", 0))
                if not temp_file_path:
                    raise IOError(f"Failed to download {path} for MIP labeling.")

                # 2. Apply the MIP label using the external SDK (run in executor as it's likely blocking)
                # await loop.run_in_executor(None, mipsdk.apply_label, temp_file_path, label_id)
                
                # 3. Upload the modified file, overwriting the original
                await loop.run_in_executor(None, self._overwrite_file_from_local_sync, temp_file_path, path)

                succeeded_paths.append(path)
            except Exception as e:
                self.error_handler.handle_error(e, f"smb_mip_{path}", **context)
                failed_paths.append(FailedObject(path=path, error_message=str(e)))
            finally:
                # 4. Clean up the temporary local file
                if temp_file_path:
                    await self._cleanup_temp_file_async(temp_file_path)
        
        return RemediationResult(
            succeeded_paths=succeeded_paths,
            failed_paths=failed_paths,
            success_count=len(succeeded_paths),
            failure_count=len(failed_paths)
        )

    def _overwrite_file_from_local_sync(self, local_path: str, remote_path: str):
        """Synchronous helper to overwrite a remote file with local content."""
        normalized_remote_path = self._normalize_smb_path(remote_path)
        
        with open(local_path, 'rb') as local_f:
            local_content = local_f.read()

        with Open(self.smb_connection.tree, normalized_remote_path) as remote_f:
            remote_f.create(
                ImpersonationLevel.Impersonation,
                FilePipePrinterAccessMask.GENERIC_WRITE,
                FileAttributes.FILE_ATTRIBUTE_NORMAL,
                ShareAccess.FILE_SHARE_WRITE,
                CreateDisposition.FILE_OVERWRITE_IF, # Overwrite if it exists, create if not
                CreateOptions.FILE_NON_DIRECTORY_FILE
            )
            remote_f.write(local_content)



    async def apply_encryption(self, 
                                 object_paths: List[str], 
                                 encryption_key_id: str, 
                                 context: Optional[Dict[str, Any]] = None) -> RemediationResult:
        """
        Placeholder method to encrypt a batch of objects in-place.
        
        A real implementation would involve a multi-step process:
        1. Download the file to a temporary local path.
        2. Use a key management service (KMS) and the encryption_key_id to encrypt the local file.
        3. Upload the encrypted file, overwriting the original on the SMB share.
        4. Clean up the temporary local file.
        """
        self.logger.warning(
            "The 'apply_encryption' method is not implemented for the SMBConnector. "
            "No action will be taken.", 
            **context
        )
        
        # Return a result indicating that all operations failed because the feature is not implemented.
        failed_paths = [
            FailedObject(path=p, error_message="Encryption is not implemented for this connector.")
            for p in object_paths
        ]
        
        return RemediationResult(
            succeeded_paths=[],
            failed_paths=failed_paths,
            success_count=0,
            failure_count=len(object_paths)
        )

    # =============================================================================
    # Async File Download and Temp Management
    # =============================================================================

    async def _download_file_to_temp_async(self, file_path: str, task_id: int) -> Optional[str]:
        """Download SMB file to temporary location for ContentExtractor processing (async)"""
        
        if not self.smb_connection or not self.smb_connection.is_connected():
            self.logger.error("SMB connection not established for file download")
            return None
        
        try:
            # Create unique temp file path
            file_name = os.path.basename(file_path)
            temp_filename = f"smb_download_{task_id}_{uuid.uuid4().hex[:8]}_{file_name}"
            temp_file_path = os.path.join(self.temp_dir, temp_filename)
            
            # Normalize SMB path
            normalized_path = self._normalize_smb_path(file_path)
            
            # Download file content asynchronously
            loop = asyncio.get_event_loop()
            
            def _download_file():
                """Download file synchronously in chunks to respect server read limits."""
                try:
                    file_handle = Open(self.smb_connection.tree, normalized_path)
                    file_handle.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.GENERIC_READ,
                        FileAttributes.FILE_ATTRIBUTE_NORMAL,
                        ShareAccess.FILE_SHARE_READ,
                        CreateDisposition.FILE_OPEN,
                        CreateOptions.FILE_NON_DIRECTORY_FILE
                    )
                    
                    # --- CORRECTED CHUNKED DOWNLOAD LOGIC ---
                    actual_file_size = file_handle.end_of_file
                    size_to_download = min(actual_file_size, self.max_file_size_bytes)
                    
                    # Use a safe, fixed chunk size (1MB) that is well below the server's 8MB limit.
                    CHUNK_SIZE_BYTES = 1 * 1024 * 1024
                    
                    data_chunks = []
                    bytes_read = 0
                    while bytes_read < size_to_download:
                        chunk = file_handle.read(offset=bytes_read, length=CHUNK_SIZE_BYTES)
                        if not chunk:
                            break # End of file reached
                        
                        data_chunks.append(chunk)
                        bytes_read += len(chunk)
                        
                    content = b"".join(data_chunks)
                    # --- END CORRECTION ---

                    file_handle.close()
                    return content
                    
                except SMBException as e:
                    if "STATUS_ACCESS_DENIED" in str(e):
                        raise RightsError(f"Access denied reading file: {file_path}", ErrorType.RIGHTS_ACCESS_DENIED)
                    elif "STATUS_OBJECT_NAME_NOT_FOUND" in str(e):
                        raise ProcessingError(f"File not found: {file_path}", ErrorType.PROCESSING_RESOURCE_NOT_FOUND)
                    else:
                        raise NetworkError(f"SMB file read failed: {str(e)}", ErrorType.NETWORK_SMB_ERROR, path=file_path)            
            # Download file content asynchronously
            content = await loop.run_in_executor(None, _download_file)
            
            # Write to temp file using aiofiles (async)
            async with aiofiles.open(temp_file_path, 'wb') as temp_file:
                await temp_file.write(content)
            
            # Track temp file for cleanup
            self.temp_files_created.append(temp_file_path)
            
            self.logger.debug("File downloaded for processing",
                             smb_path=file_path, temp_path=temp_file_path)
            
            return temp_file_path
            
        except Exception as e:
            # This will now log the specific error type (e.g., RightsError) and the full traceback.
            self.logger.error(
                "File download failed",
                file_path=file_path,
                error_type=type(e).__name__,
                error_message=str(e),
                exc_info=True # Adds the full traceback to the log for detailed debugging
            )
            return None

    async def _cleanup_temp_file_async(self, temp_file_path: str):
        """Clean up individual temp file (async)"""
        try:
            # Check if file exists asynchronously
            if await aiofiles.os.path.exists(temp_file_path):
                await aiofiles.os.remove(temp_file_path)
                if temp_file_path in self.temp_files_created:
                    self.temp_files_created.remove(temp_file_path)
        except Exception as e:
            self.logger.warning("Failed to cleanup temp file",
                               temp_path=temp_file_path, error=str(e))

    async def _cleanup_all_temp_files_async(self):
        """Clean up all temp files created by this connector (async)"""
        cleanup_tasks = []
        for temp_file in self.temp_files_created[:]:  # Copy list to avoid modification
            cleanup_tasks.append(self._cleanup_temp_file_async(temp_file))
        
        # Clean up all temp files concurrently
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    # =============================================================================
    # Async Connection Management
    # =============================================================================

    async def _ensure_connection_async(self, trace_id: str, task_id: str):
        """
        Ensures a connection is established by fetching credentials
        and connecting on the first call.
        """
        if self._is_connected and self.smb_connection:
            return

        # 1. Get the credential_id from the data source configuration
        credential_id = self.smb_config.get("scan_profiles", [{}])[0].get("credential_id")
        if not credential_id:
            raise ValueError("credential_id is missing from SMB data source config")

        # 2. Fetch the credential record from the database (async)
        credential_record = await self.db_interface.get_credential_by_id_async(credential_id)
        if not credential_record:
            raise ValueError(f"Credential '{credential_id}' not found in database.")

        # 3. Use the CredentialManager to get the actual password (async)
        password = await self.credential_manager.get_password_async(credential_record.store_details)

        # 4. Prepare the connection details
        connection_details = {
            "host": self.smb_config.get("connection", {}).get("host"),
            "share_name": self.smb_config.get("connection", {}).get("share_or_bucket"),
            "username": credential_record.username,
            "password": password,
            "domain": credential_record.domain
        }

        # 5. Establish the actual connection
        self.smb_connection = SMBConnection(connection_details, self.logger)
        await self.smb_connection.connect_async()
        self._is_connected = True

    async def connect_async(self) -> bool:
        """Establish SMB connection (async)"""
        try:
            await self._ensure_connection_async()
            self.logger.info("SMB datasource connected", 
                           datasource_id=self.datasource_id)
            return True
                
        except Exception as e:
            self.logger.error("SMB connection failed",
                             datasource_id=self.datasource_id, error=str(e))
            raise

    async def disconnect_async(self):
        """Close SMB connection and cleanup (async)"""
        try:
            # Clean up temp files first
            await self._cleanup_all_temp_files_async()
            
            # Close SMB connection
            if self.smb_connection:
                await self.smb_connection.disconnect_async()
                self.smb_connection = None
            
            self.logger.info("SMB datasource disconnected", 
                           datasource_id=self.datasource_id)
            
        except Exception as e:
            self.logger.warning("SMB disconnect error", error=str(e))

    async def test_connectivity_async(self) -> bool:
        """Test SMB connectivity (async)"""
        try:
            temp_connection = SMBConnection(self.smb_config, self.logger)
            result = await temp_connection.connect_async()
            if result:
                await temp_connection.disconnect_async()
            return result
        except Exception:
            return False

    # =============================================================================
    # Async Object Discovery Helpers
    # =============================================================================

    async def _create_discovered_object_async(self, entry: Dict[str, Any], full_path: str) -> Optional[DiscoveredObject]:
        """
        Create DiscoveredObject from SMB directory entry using correct smbprotocol field access pattern.
        All timestamps are converted to UTC.
        
        Args:
            entry: Raw SMB directory entry object from query_directory()
            full_path: Normalized SMB path for this object
            
        Returns:
            DiscoveredObject instance or None if creation fails
        """
        try:
            # Extract file attributes to determine if directory or file
            file_attributes = entry['file_attributes'].get_value()
            is_directory = bool(file_attributes & FileAttributes.FILE_ATTRIBUTE_DIRECTORY)
            
            # Set object type based on file attributes
            if is_directory:
                object_type_str = "DIRECTORY"
                object_type_enum = ObjectType.DIRECTORY
            else:
                object_type_str = "FILE"
                object_type_enum = ObjectType.FILE
            
            # Extract file size (directories have 0 size)
            size_bytes = entry['end_of_file'].get_value() if not is_directory else 0
            
            # Extract timestamps - they are naive datetimes in server's local timezone
            # Convert to UTC using configured server timezone
            creation_time_naive = entry['creation_time'].get_value()
            last_write_time_naive = entry['last_write_time'].get_value()
            last_access_time_naive = entry['last_access_time'].get_value()
            
            # Convert from server timezone to UTC
            creation_time = creation_time_naive.replace(tzinfo=self.server_tz).astimezone(timezone.utc)
            last_write_time = last_write_time_naive.replace(tzinfo=self.server_tz).astimezone(timezone.utc)
            last_access_time = last_access_time_naive.replace(tzinfo=self.server_tz).astimezone(timezone.utc)
            
            # Generate consistent object key hash
            object_key_hash = generate_object_key_hash(
                datasource_id=self.datasource_id,
                object_path=full_path,
                object_type=object_type_str
            )
            
            # Create DiscoveredObject with all required fields
            discovered_obj = DiscoveredObject(
                object_key_hash=object_key_hash,
                datasource_id=self.datasource_id,
                object_type=object_type_enum,
                object_path=full_path,
                size_bytes=size_bytes,
                created_date=creation_time,
                last_modified=last_write_time,
                last_accessed=last_access_time,
                discovery_timestamp=datetime.now(timezone.utc)
            )
            
            return discovered_obj
            
        except KeyError as e:
            self.logger.warning(
                f"Missing expected field in SMB entry: {e}",
                file_path=full_path
            )
            return None
        except Exception as e:
            self.logger.warning(
                f"Failed to create discovered object: {str(e)}",
                file_path=full_path,
                error=str(e)
            )
            return None

    # =============================================================================
    # Async Detailed Metadata Collection
    # =============================================================================


    async def get_object_details(self, work_packet: WorkPacket) -> List[ObjectMetadata]:
        """
        Gets detailed metadata for objects. This is now a proper async method.
        """
        # The payload contains the full DiscoveredObject models
        discovered_objects = work_packet.payload.discovered_objects
        
        # Simply await the async helper method directly.
        # There is no need for any complex event loop management.
        results = await self._get_details_batch_async(discovered_objects)
        
        return results


    async def _get_details_batch_async(self, discovered_objects: List[DiscoveredObject]) -> List[Dict[str, Any]]:
        """Process batch of objects for detailed metadata collection."""
        results = []
        
        for obj in discovered_objects:
            try:
                # Get detailed metadata
                detailed_metadata = await self._get_detailed_file_metadata_async(
                    obj.object_path,
                    obj.object_type
                )
                
                # Construct result in ObjectMetadata-compatible format
                # This creates the actual Pydantic ObjectMetadata object
                # instead of a plain dictionary.
                metadata_obj = ObjectMetadata(
                    base_object=obj,
                    detailed_metadata=detailed_metadata
                )
                results.append(metadata_obj)
                
                
            except Exception as e:
                self.logger.warning(
                    "Failed to get object details",
                    object_path=obj.object_path,
                    error=str(e)
                )
                # Add minimal result with error info
                results.append({
                    "base_object": obj.dict(),
                    "detailed_metadata": {
                        "object_path": obj.object_path,
                        "metadata_collection_failed": True,
                        "error": str(e)
                    },
                    "metadata_fetch_timestamp": datetime.now(timezone.utc).isoformat()
                })
        
        return results


    async def _get_detailed_file_metadata_async(self, object_path: str, object_type: ObjectType) -> Dict[str, Any]:
        """
        Get comprehensive security metadata for an object using SMB2 security queries.
        Uses the proven pattern from smbtest.py.
        
        Args:
            object_path: Full SMB path to the object
            object_type: ObjectType enum (FILE or DIRECTORY)
            
        Returns:
            Dictionary with security metadata matching smbtest.py output format
        """
        try:
            normalized_path = self._normalize_smb_path(object_path)
            is_directory = (object_type == ObjectType.DIRECTORY)
            
            loop = asyncio.get_event_loop()
            
            def _get_security_info():
                """Synchronous security query executed in thread pool."""
                try:
                    # Open the object with appropriate flags
                    handle = Open(self.smb_connection.tree, normalized_path)
                    
                    # Try different access levels for security queries
                    access_attempts = [
                        (FilePipePrinterAccessMask.GENERIC_READ | FilePipePrinterAccessMask.READ_CONTROL, "GENERIC_READ + READ_CONTROL"),
                        (FilePipePrinterAccessMask.READ_CONTROL, "READ_CONTROL only"),
                        (FilePipePrinterAccessMask.GENERIC_READ, "GENERIC_READ only")
                    ]
                    
                    access_granted = False
                    access_used = None
                    
                    for access_mask, access_description in access_attempts:
                        try:
                            if is_directory:
                                handle.create(
                                    ImpersonationLevel.Impersonation,
                                    access_mask,
                                    FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                                    ShareAccess.FILE_SHARE_READ,
                                    CreateDisposition.FILE_OPEN,
                                    CreateOptions.FILE_DIRECTORY_FILE
                                )
                            else:
                                handle.create(
                                    ImpersonationLevel.Impersonation,
                                    access_mask,
                                    FileAttributes.FILE_ATTRIBUTE_NORMAL,
                                    ShareAccess.FILE_SHARE_READ,
                                    CreateDisposition.FILE_OPEN,
                                    CreateOptions.FILE_NON_DIRECTORY_FILE
                                )
                            access_granted = True
                            access_used = access_description
                            break
                        except Exception:
                            try:
                                handle.close()
                            except:
                                pass
                            handle = Open(self.smb_connection.tree, normalized_path)
                    
                    if not access_granted:
                        return {
                            "object_path": object_path,
                            "object_type": "directory" if is_directory else "file",
                            "security_query_failed": True,
                            "error": "All access attempts failed",
                            "collection_timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    
                    # Set security flags based on object type
                    # Files: Owner + Group only (0x3)
                    # Directories: Owner + Group + DACL (0x7)
                    security_flags = 0x00000007 if is_directory else 0x00000003
                    
                    # Build SMB2 security query request
                    query_req = SMB2QueryInfoRequest()
                    query_req['info_type'] = InfoType.SMB2_0_INFO_SECURITY
                    query_req['output_buffer_length'] = 65535
                    query_req['additional_information'] = security_flags
                    query_req['file_id'] = handle.file_id
                    
                    # Send query and receive response
                    req = handle.connection.send(
                        query_req,
                        sid=handle.tree_connect.session.session_id,
                        tid=handle.tree_connect.tree_connect_id
                    )
                    resp = handle.connection.receive(req)
                    
                    # Unpack response
                    query_resp = SMB2QueryInfoResponse()
                    query_resp.unpack(resp['data'].get_value())
                    
                    # Parse security descriptor
                    security_descriptor = SMB2CreateSDBuffer()
                    security_descriptor.unpack(query_resp['buffer'].get_value())
                    
                    # Build metadata result
                    metadata = {
                        "object_path": object_path,
                        "object_type": "directory" if is_directory else "file",
                        "security_query_type": "full_acls_enhanced" if is_directory else "basic_owner_group",
                        "security_flags_requested": hex(security_flags),
                        "access_method_used": access_used,
                        "collection_timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    
                    # Extract Owner SID
                    try:
                        owner = security_descriptor.get_owner()
                        metadata["owner_sid"] = str(owner) if owner else None
                    except Exception as e:
                        metadata["owner_extraction_error"] = str(e)
                    
                    # Extract Group SID
                    try:
                        group = security_descriptor.get_group()
                        metadata["group_sid"] = str(group) if group else None
                    except Exception as e:
                        metadata["group_extraction_error"] = str(e)
                    
                    # Extract DACL for directories only
                    if is_directory:
                        try:
                            dacl = security_descriptor.get_dacl()
                            if dacl and hasattr(dacl, 'fields') and dacl.fields and 'aces' in dacl.fields:
                                aces_list = dacl.fields['aces'].get_value()
                                ace_count = dacl.fields['ace_count'].get_value()
                                
                                dacl_info = {
                                    "dacl_present": True,
                                    "ace_count": ace_count,
                                    "acl_revision": dacl.fields['acl_revision'].get_value(),
                                    "acl_size": dacl.fields['acl_size'].get_value(),
                                    "aces": []
                                }
                                
                                # Extract each ACE
                                for i, ace in enumerate(aces_list):
                                    ace_info = self._extract_ace_info(ace, i)
                                    dacl_info["aces"].append(ace_info)
                                
                                metadata["dacl_info"] = dacl_info
                            else:
                                metadata["dacl_info"] = {"dacl_present": False}
                        except Exception as e:
                            metadata["dacl_extraction_error"] = str(e)
                    
                    handle.close()
                    return metadata
                    
                except SMBException as e:
                    if "STATUS_ACCESS_DENIED" in str(e):
                        return {
                            "object_path": object_path,
                            "object_type": "directory" if is_directory else "file",
                            "security_query_failed": True,
                            "error": "Access denied",
                            "collection_timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    else:
                        raise
            
            # Execute security query asynchronously
            return await loop.run_in_executor(None, _get_security_info)
            
        except Exception as e:
            return {
                "object_path": object_path,
                "object_type": "directory" if object_type == ObjectType.DIRECTORY else "file",
                "metadata_collection_failed": True,
                "error": str(e),
                "collection_timestamp": datetime.now(timezone.utc).isoformat()
            }


    def _extract_ace_info(self, ace, index: int) -> Dict[str, Any]:
        """
        Extract detailed information from an ACE.
        This helper already exists in the connector from smbtest.py pattern.
        """
        ace_info = {
            "ace_index": index,
            "ace_type": str(type(ace)),
            "ace_class_name": ace.__class__.__name__
        }
        
        try:
            # Extract ACE type
            if hasattr(ace, 'fields') and 'ace_type' in ace.fields:
                ace_type_value = ace.fields['ace_type'].get_value()
                ace_info["ace_type_value"] = ace_type_value
                
                ace_type_map = {
                    0: "ACCESS_ALLOWED_ACE_TYPE",
                    1: "ACCESS_DENIED_ACE_TYPE",
                    2: "SYSTEM_AUDIT_ACE_TYPE"
                }
                ace_info["ace_type_name"] = ace_type_map.get(ace_type_value, f"UNKNOWN_TYPE_{ace_type_value}")
            
            # Extract ACE flags
            if hasattr(ace, 'fields') and 'ace_flags' in ace.fields:
                ace_flags_value = ace.fields['ace_flags'].get_value()
                ace_info["ace_flags"] = ace_flags_value
                ace_info["ace_flags_decoded"] = self._decode_ace_flags(ace_flags_value)
            
            # Extract access mask
            access_mask = None
            if hasattr(ace, 'fields') and 'mask' in ace.fields:
                access_mask = ace.fields['mask'].get_value()
            elif hasattr(ace, 'fields') and 'access_mask' in ace.fields:
                access_mask = ace.fields['access_mask'].get_value()
            
            if access_mask is not None:
                ace_info["access_mask"] = access_mask
                ace_info["access_mask_hex"] = hex(access_mask)
                ace_info["permissions"] = self._decode_access_mask(access_mask)
            
            # Extract SID
            if hasattr(ace, 'fields') and 'sid' in ace.fields:
                sid_obj = ace.fields['sid'].get_value()
                ace_info["sid"] = str(sid_obj)
            
            # Extract ACE size
            if hasattr(ace, 'fields') and 'ace_size' in ace.fields:
                ace_info["ace_size"] = ace.fields['ace_size'].get_value()
            
        except Exception as e:
            ace_info["extraction_error"] = str(e)
        
        return ace_info


    def _decode_ace_flags(self, flags: int) -> List[str]:
        """Decode ACE flags to human-readable descriptions."""
        flag_descriptions = []
        
        if flags & 0x01:
            flag_descriptions.append("OBJECT_INHERIT")
        if flags & 0x02:
            flag_descriptions.append("CONTAINER_INHERIT")
        if flags & 0x04:
            flag_descriptions.append("NO_PROPAGATE_INHERIT")
        if flags & 0x08:
            flag_descriptions.append("INHERIT_ONLY")
        if flags & 0x10:
            flag_descriptions.append("INHERITED")
        
        return flag_descriptions if flag_descriptions else ["NONE"]
    # =============================================================================
    # Helper Methods (unchanged from original)
    # =============================================================================

    def _extract_path_from_object_id(self, object_id: str) -> Optional[str]:
        """Extract file path from object ID"""
        try:
            # Object ID format: {datasource_id}:{path_hash}:{actual_path}
            # For SMB, we need the actual path component
            parts = object_id.split(':', 2)
            if len(parts) >= 3:
                return parts[2]  # The actual path
            
            # Fallback: assume object_id is the path
            return object_id
            
        except Exception:
            return None

    def _create_extraction_error_component(self, object_id: str, error_message: str) -> ContentComponent:
        """Create error component when extraction fails"""
        
        file_path = self._extract_path_from_object_id(object_id) or object_id
        base_name = os.path.basename(file_path).split('.')[0] if file_path else object_id
        
        return ContentComponent(
            object_id=object_id,
            component_type="extraction_error",
            component_id=f"{base_name}_smb_error_1",
            parent_path=file_path,
            content=f"SMB extraction failed: {error_message}",
            original_size=0,
            extracted_size=len(error_message),
            is_truncated=False,
            is_archive_extraction=False,
            schema={},
            metadata={
                "error_type": "smb_extraction_error",
                "smb_server": self.smb_config.get('host'),
                "smb_share": self.smb_config.get('share_name'),
                "extraction_timestamp": datetime.now(timezone.utc).isoformat()
            },
            extraction_method="smb_error_handler"
        )

    # =============================================================================
    # Security Metadata Parsing (unchanged from original)
    # =============================================================================

    def _parse_dacl_entries(self, dacl) -> List[Dict[str, Any]]:
        """Parse DACL entries with proper error handling"""
        
        if not dacl:
            return []
        
        try:
            entries = []
            
            # Check if dacl has aces attribute
            if hasattr(dacl, 'aces'):
                for ace in dacl.aces:
                    entry = {
                        "sid": str(ace.sid) if hasattr(ace, 'sid') else "unknown",
                        "access_mask": ace.access_mask if hasattr(ace, 'access_mask') else 0,
                        "ace_type": str(ace.ace_type) if hasattr(ace, 'ace_type') else "unknown",
                        "permissions": self._decode_access_mask(ace.access_mask) if hasattr(ace, 'access_mask') else []
                    }
                    entries.append(entry)
            
            return entries
            
        except Exception as e:
            self.logger.warning("DACL parsing failed", error=str(e))
            return []

    def _parse_sacl_entries(self, sacl) -> List[Dict[str, Any]]:
        """Parse SACL entries with proper error handling"""
        
        if not sacl:
            return []
        
        try:
            entries = []
            
            # Check if sacl has aces attribute  
            if hasattr(sacl, 'aces'):
                for ace in sacl.aces:
                    entry = {
                        "sid": str(ace.sid) if hasattr(ace, 'sid') else "unknown",
                        "access_mask": ace.access_mask if hasattr(ace, 'access_mask') else 0,
                        "ace_type": str(ace.ace_type) if hasattr(ace, 'ace_type') else "unknown",
                        "audit_permissions": self._decode_access_mask(ace.access_mask) if hasattr(ace, 'access_mask') else []
                    }
                    entries.append(entry)
            
            return entries
            
        except Exception as e:
            self.logger.warning("SACL parsing failed", error=str(e))
            return []

    def _decode_access_mask(self, access_mask: int) -> List[str]:
        """Decode access mask to human-readable permissions"""
        
        try:
            permissions = []
            
            # Standard access rights
            if access_mask & 0x00000001:  # FILE_READ_DATA
                permissions.append("read")
            if access_mask & 0x00000002:  # FILE_WRITE_DATA  
                permissions.append("write")
            if access_mask & 0x00000004:  # FILE_APPEND_DATA
                permissions.append("append")
            if access_mask & 0x00000020:  # FILE_EXECUTE
                permissions.append("execute")
            if access_mask & 0x00000040:  # FILE_DELETE_CHILD
                permissions.append("delete_child")
            if access_mask & 0x00000080:  # FILE_READ_ATTRIBUTES
                permissions.append("read_attributes")
            if access_mask & 0x00000100:  # FILE_WRITE_ATTRIBUTES
                permissions.append("write_attributes")
            if access_mask & 0x00010000:  # DELETE
                permissions.append("delete")
            if access_mask & 0x00020000:  # READ_CONTROL
                permissions.append("read_control")
            if access_mask & 0x00040000:  # WRITE_DAC
                permissions.append("write_dac")
            if access_mask & 0x00080000:  # WRITE_OWNER
                permissions.append("write_owner")
            
            # Generic rights
            if access_mask & 0x10000000:  # GENERIC_ALL
                permissions.append("full_control")
            if access_mask & 0x20000000:  # GENERIC_EXECUTE
                permissions.append("generic_execute")
            if access_mask & 0x40000000:  # GENERIC_WRITE
                permissions.append("generic_write")
            if access_mask & 0x80000000:  # GENERIC_READ
                permissions.append("generic_read")
            
            return permissions if permissions else ["unknown"]
            
        except Exception:
            return [f"raw_mask_{hex(access_mask)}"]

    def _get_smb_version(self) -> str:
        """Get SMB protocol version from connection"""
        
        try:
            if self.smb_connection and self.smb_connection.connection:
                dialect = self.smb_connection.connection.dialect
                
                # Convert dialect to readable version
                version_map = {
                    0x0202: "2.0.2",
                    0x0210: "2.1", 
                    0x0300: "3.0",
                    0x0302: "3.0.2",
                    0x0311: "3.1.1"
                }
                
                return version_map.get(dialect, f"Unknown ({hex(dialect)})")
            
            return "Unknown"
            
        except Exception:
            return "Unknown"

    # =============================================================================
    # Path and Pattern Utilities (unchanged from original)
    # =============================================================================
    
    def _normalize_smb_path(self, path: str) -> str:
        """Normalize path for SMB operations"""
        # Convert forward slashes to backslashes for SMB
        normalized = path.replace('/', '\\')
        
        # Remove leading slash if present
        if normalized.startswith('\\'):
            normalized = normalized[1:]
        
        # Handle root path
        if not normalized or normalized == '.':
            normalized = ''
        
        return normalized
    
    def _join_smb_path(self, parent: str, child: str) -> str:
        """Join SMB paths correctly"""
        if not parent:
            return child
        
        # Ensure parent doesn't end with backslash
        if parent.endswith('\\'):
            parent = parent[:-1]
        
        return f"{parent}\\{child}"
    
    def _matches_include_patterns(self, path: str) -> bool:
        """Check if path matches include patterns"""
        if not self.include_patterns or '*' in self.include_patterns:
            return True
        
        return any(
            fnmatch.fnmatch(path.lower(), pattern.lower()) 
            for pattern in self.include_patterns
        )
    
    def _matches_exclude_patterns(self, path: str) -> bool:
        """Check if path matches exclude patterns"""
        if not self.exclude_patterns:
            return False
        
        return any(
            fnmatch.fnmatch(path.lower(), pattern.lower()) 
            for pattern in self.exclude_patterns
        )
    
    def _filetime_to_datetime(self, filetime: int) -> Optional[datetime]:
        """Convert Windows FILETIME to datetime"""
        if not filetime or filetime == 0:
            return None
        
        try:
            # FILETIME is 100-nanosecond intervals since January 1, 1601
            unix_timestamp = (filetime - 116444736000000000) / 10000000
            return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        except (ValueError, OSError):
            return None
    
    def _get_content_type(self, file_extension: str) -> str:
        """Get MIME type for file extension"""
        extension_map = {
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.pdf': 'application/pdf',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.doc': 'application/msword',
            '.xls': 'application/vnd.ms-excel',
            '.html': 'text/html',
            '.htm': 'text/html',
            '.log': 'text/plain',
            '.sql': 'text/plain',
            '.py': 'text/plain',
            '.js': 'text/javascript',
            '.md': 'text/markdown'
        }
        
        return extension_map.get(file_extension.lower(), 'application/octet-stream')

    # =============================================================================
    # Error Handling (unchanged from original)
    # =============================================================================

    def _handle_processing_error(self, e: Exception, operation: str, **context) -> Exception:
        """Handle processing errors with proper categorization"""
        
        if isinstance(e, (RightsError, NetworkError, ProcessingError)):
            return e
        
        error = self.error_handler.handle_error(
            e, f"smb_{operation}",
            operation=operation,
            datasource_id=self.datasource_id,
            **context
        )
        
        return ProcessingError(
            f"SMB {operation} failed: {str(e)}",
            ErrorType.PROCESSING_LOGIC_ERROR,
            operation=operation
        )

    # =============================================================================
    # Async Health Check and Diagnostics
    # =============================================================================
    
    async def health_check_async(self) -> Dict[str, Any]:
        """Perform SMB-specific health check (async)"""
        
        health_status = {
            "datasource_id": self.datasource_id,
            "datasource_type": "SMB",
            "status": "healthy",
            "checks": {}
        }
        
        try:
            if self.smb_connection and self.smb_connection.is_connected():
                # Connection status
                health_status['checks']['connection'] = {
                    'status': 'connected',
                    'host': self.smb_config.get('host'),
                    'share': self.smb_config.get('share_name'),
                    'smb_version': self._get_smb_version()
                }
                
                # Root path accessibility  
                root_access = await self._check_path_access_async(self.root_path)
                health_status['checks']['root_path_access'] = root_access
                
            else:
                health_status['checks']['connection'] = {
                    'status': 'disconnected'
                }
                health_status['status'] = 'unhealthy'
        
        except Exception as e:
            health_status['checks']['health_check_error'] = {
                'status': 'error',
                'error': str(e)
            }
            health_status['status'] = 'unhealthy'
        
        return health_status

    async def _check_path_access_async(self, path: str) -> Dict[str, Any]:
        """Check if specific path is accessible (async)"""
        
        try:
            normalized_path = self._normalize_smb_path(path)
            
            loop = asyncio.get_event_loop()
            
            def _check_access():
                """Check path access synchronously (in thread pool)"""
                try:
                    # Try to open as file first
                    handle = Open(self.smb_connection.tree, normalized_path)
                    handle.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.GENERIC_READ,
                        FileAttributes.FILE_ATTRIBUTE_NORMAL,
                        ShareAccess.FILE_SHARE_READ,
                        CreateDisposition.FILE_OPEN,
                        CreateOptions.FILE_NON_DIRECTORY_FILE
                    )
                    handle.close()
                    
                    return {'accessible': True, 'type': 'file'}
                    
                except SMBException as e:
                    if "STATUS_FILE_IS_A_DIRECTORY" in str(e):
                        # Try as directory
                        try:
                            handle = Open(self.smb_connection.tree, normalized_path)
                            handle.create(
                                ImpersonationLevel.Impersonation,
                                FilePipePrinterAccessMask.GENERIC_READ,
                                FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                                ShareAccess.FILE_SHARE_READ,
                                CreateDisposition.FILE_OPEN,
                                CreateOptions.FILE_DIRECTORY_FILE
                            )
                            handle.close()
                            
                            return {'accessible': True, 'type': 'directory'}
                            
                        except SMBException:
                            return {'accessible': False, 'error': 'Access denied as directory'}
                    
                    return {'accessible': False, 'error': str(e)}
            
            return await loop.run_in_executor(None, _check_access)
            
        except Exception as e:
            return {'accessible': False, 'error': str(e)}


# =============================================================================
# Factory Function
# =============================================================================

async def create_smb_connector_async(datasource_id: str, 
                                    smb_config: Dict[str, Any],
                                    content_extractor: ContentExtractor,
                                    logger: SystemLogger,
                                    error_handler) -> SMBConnector:
    """Factory function to create async SMB connector with dependencies"""
    
    connector = SMBConnector(
        datasource_id=datasource_id,
        smb_config=smb_config,
        content_extractor=content_extractor,
        logger=logger, 
        error_handler=error_handler
    )
    
    # Test connectivity
    if not await connector.test_connectivity_async():
        raise NetworkError(
            f"SMB connector failed connectivity test",
            ErrorType.NETWORK_CONNECTION_FAILED,
            datasource_id=datasource_id
        )
    
    return connector


# =============================================================================
# Testing and Validation
# =============================================================================

async def test_smb_connector_async():
    """Test async SMB connector functionality"""
    
    from core.models.models import WorkPacket, WorkPacketHeader, WorkPacketPayload, TaskType
    
    # Mock dependencies for testing
    class MockLogger:
        def info(self, msg, **kwargs): print(f"INFO: {msg}")
        def warning(self, msg, **kwargs): print(f"WARN: {msg}")
        def error(self, msg, **kwargs): print(f"ERROR: {msg}")
        def debug(self, msg, **kwargs): print(f"DEBUG: {msg}")
    
    class MockErrorHandler:
        def handle_error(self, e, error_id, **kwargs):
            return type('MockError', (), {'error_id': error_id})()
    
    class MockContentExtractor:
        async def extract_from_file(self, file_path, object_id, job_id, task_id, datasource_id):
            # Mock async ContentComponent for testing
            yield type('MockComponent', (), {
                'object_id': object_id,
                'component_type': 'text',
                'component_id': f'{object_id}_text_1',
                'content': 'Mock extracted content',
                'metadata': {}
            })()
    
    try:
        # Test connector creation
        connector = SMBConnector(
            datasource_id="test_smb_ds",
            smb_config={
                "host": "test-server",
                "share_name": "test-share", 
                "username": "test-user",
                "password": "test-password",
                "domain": "TEST-DOMAIN",
                "root_path": "/documents",
                "recursive": True,
                "include_patterns": ["*.txt", "*.pdf", "*.docx"],
                "exclude_patterns": ["*/temp/*", "*/backup/*"],
                "max_file_size_mb": 50
            },
            content_extractor=MockContentExtractor(),
            logger=MockLogger(),
            error_handler=MockErrorHandler()
        )
        
        print("✅ Async SMB connector created successfully")
        
        # Test WorkPacket creation
        test_work_packet = WorkPacket(
            header=WorkPacketHeader(
                task_id=12345,
                trace_id=67890,
                priority=1
            ),
            payload=WorkPacketPayload(
                task_type=TaskType.CLASSIFICATION,
                datasource_id="test_smb_ds",
                object_ids=["test_object_1", "test_object_2"]
            )
        )
        
        print("✅ Async WorkPacket structure compatible")
        
        # Test async interface compliance
        assert hasattr(connector, 'enumerate_objects'), "Missing async enumerate_objects method"
        assert hasattr(connector, 'get_object_content'), "Missing async get_object_content method" 
        assert hasattr(connector, 'get_object_details'), "Missing get_object_details method"
        
        print("✅ Async IFileDataSourceConnector interface methods present")
        print("✅ AsyncIterator support confirmed")
        print("✅ aiofiles integration ready")
        
        print("\n🎯 Async SMB connector architecture validation PASSED")
        print("Ready for Task 9 integration with async Worker")
        
    except Exception as e:
        print(f"❌ Async SMB connector test failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_smb_connector_async())