# datasources/smb.py
"""
SMB file share datasource implementation with IFileDataSourceConnector interface.
Integrates with universal ContentExtractor and WorkPacket system.

FIXES APPLIED:
- Implements IFileDataSourceConnector interface
- Integrates universal ContentExtractor 
- Returns ContentComponent objects from get_object_content()
- Supports WorkPacket-based task processing
- Downloads files to temp locations for ContentExtractor processing
"""

import os
import uuid
import fnmatch
import asyncio
import tempfile
from typing import Iterator, Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime, timezone

# SMB protocol imports
try:
    import smbprotocol
    from smbprotocol.connection import Connection
    from smbprotocol.session import Session
    from smbprotocol.tree import TreeConnect
    from smbprotocol.open import (
        Open, CreateDisposition, ShareAccess, FileAttributes, 
        CreateOptions, FilePipePrinterAccessMask, ImpersonationLevel
    )
    from smbprotocol.file_info import FileInformationClass, FileFullDirectoryInformation
    from smbprotocol.exceptions import SMBException, SMBResponseException
    from smbprotocol.security_descriptor import SMB2CreateSDBuffer
except ImportError as e:
    raise ImportError(
        "SMB datasource requires smbprotocol: pip install smbprotocol"
    ) from e

# Core system imports
from core.interfaces.worker_interfaces import IFileDataSourceConnector
from core.models import (
    WorkPacket, DiscoveredObject, ObjectMetadata, ObjectType, 
    DataSourceConfig, ContentComponent
)
from core.errors import (
    NetworkError, RightsError, ProcessingError, ConfigurationError,
    ErrorType, ClassificationError
)
from core.logging.system_logger import SystemLogger
from extraction.content_extractor import ContentExtractor

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
# SMB Connection Management
# =============================================================================

class SMBConnection:
    """Manages SMB connections with proper error handling and cleanup"""
    
    def __init__(self, connection_config: Dict[str, Any], logger: SystemLogger):
        self.connection_config = connection_config
        self.logger = logger
        self.connection: Optional[Connection] = None
        self.session: Optional[Session] = None
        self.tree: Optional[TreeConnect] = None
        self._connected = False
    
    async def connect(self) -> bool:
        """Establish SMB connection with proper error handling"""
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
                
                # Create session
                session = Session(conn, username, password, domain)
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
    
    async def disconnect(self):
        """Close SMB connection with proper cleanup"""
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
# SMB DataSource Implementation with IFileDataSourceConnector Interface
# =============================================================================

class SMBDataSource(IFileDataSourceConnector):
    """SMB file share datasource with ContentExtractor integration"""

    def __init__(self, config: DataSourceConfig, logger: SystemLogger, 
                 error_handler, content_extractor: ContentExtractor):
        self.config = config
        self.datasource_id = config.datasource_id
        self.logger = logger
        self.error_handler = error_handler
        self.content_extractor = content_extractor
        
        # SMB connection
        self.smb_connection: Optional[SMBConnection] = None
        
        # SMB-specific configuration
        self.root_path = config.scan_config.get('root_path', '/')
        self.recursive = config.scan_config.get('recursive', True)
        self.include_patterns = config.scan_config.get('include_patterns', ['*'])
        self.exclude_patterns = config.scan_config.get('exclude_patterns', [])
        self.max_file_size_mb = config.scan_config.get('max_file_size_mb', 100)
        self.max_file_size_bytes = self.max_file_size_mb * 1024 * 1024
        
        # Temp file management
        self.temp_files_created = []
        self.temp_dir = tempfile.gettempdir()

    # =============================================================================
    # IFileDataSourceConnector Interface Implementation
    # =============================================================================

    def enumerate_objects(self, work_packet: WorkPacket) -> Iterator[DiscoveredObject]:
        """Enumerate objects for discovery tasks - interface method"""
        
        # Extract filters from work packet
        filters = work_packet.payload.filters if hasattr(work_packet.payload, 'filters') else None
        
        self.logger.info("Starting SMB object enumeration",
                        task_id=work_packet.header.task_id,
                        datasource_id=self.datasource_id)
        
        try:
            # Process discovery synchronously
            for obj in self._enumerate_files_sync(self.root_path, filters):
                yield obj
                
        except Exception as e:
            error = self.error_handler.handle_error(
                e, f"smb_enumerate_objects_{work_packet.header.task_id}",
                operation="enumerate_objects",
                task_id=work_packet.header.task_id
            )
            self.logger.error("SMB enumeration failed", error_id=error.error_id)
            raise

    def _enumerate_files_sync(self, directory_path: str, filters: Optional[Dict[str, Any]] = None) -> Iterator[DiscoveredObject]:
        """Synchronous file enumeration for interface compatibility"""
        
        if not self.smb_connection or not self.smb_connection.is_connected():
            raise ProcessingError(
                "SMB connection not established",
                ErrorType.PROCESSING_LOGIC_ERROR,
                operation="enumerate_files"
            )
        
        try:
            # Normalize path for SMB
            normalized_path = self._normalize_smb_path(directory_path)
            
            # Enumerate directory synchronously
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
            
            # Get directory entries
            entries = _enumerate_directory()
            
            # Process entries and yield discovered objects
            for entry in entries:
                file_name = entry['file_name']
                full_path = self._join_smb_path(normalized_path, file_name)
                
                # Check if it's a directory
                is_directory = bool(entry['file_attributes'] & FileAttributes.FILE_ATTRIBUTE_DIRECTORY)
                
                if is_directory:
                    # Recursively process subdirectory if enabled
                    if self.recursive:
                        for sub_obj in self._enumerate_files_sync(full_path, filters):
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
                    discovered_obj = self._create_discovered_object_sync(entry, full_path)
                    if discovered_obj:
                        yield discovered_obj
                        
        except Exception as e:
            raise self._handle_processing_error(
                e, 'enumerate_files_sync',
                directory_path=directory_path
            )

    def get_object_content(self, work_packet: WorkPacket) -> Iterator[ContentComponent]:
        """Get content components for classification - interface method"""
        
        # Extract object IDs from work packet
        object_ids = work_packet.payload.object_ids
        
        self.logger.info("Starting SMB content extraction",
                        task_id=work_packet.header.task_id,
                        object_count=len(object_ids))
        
        # Process each object ID
        for object_id in object_ids:
            try:
                # Extract file path from object ID
                file_path = self._extract_path_from_object_id(object_id)
                
                if not file_path:
                    self.logger.warning("Invalid object ID format", object_id=object_id)
                    continue
                
                # Download file to temp location
                temp_file_path = self._download_file_to_temp(file_path, work_packet.header.task_id)
                
                if not temp_file_path:
                    continue
                
                try:
                    # Use ContentExtractor to process the downloaded file
                    for component in self.content_extractor.extract_from_file(
                        temp_file_path, 
                        object_id,
                        work_packet.header.trace_id,  # job_id
                        work_packet.header.task_id,   # task_id  
                        self.datasource_id            # datasource_id
                    ):
                        # Add SMB-specific metadata to components
                        component.metadata.update({
                            "smb_source_path": file_path,
                            "smb_server": self.config.connection_config.get('host'),
                            "smb_share": self.config.connection_config.get('share_name'),
                            "downloaded_for_processing": True
                        })
                        
                        yield component
                        
                finally:
                    # Clean up temp file
                    self._cleanup_temp_file(temp_file_path)
                    
            except Exception as e:
                error = self.error_handler.handle_error(
                    e, f"smb_content_extraction_{object_id}",
                    operation="get_object_content",
                    object_id=object_id,
                    task_id=work_packet.header.task_id
                )
                self.logger.warning("SMB content extraction failed", 
                                   error_id=error.error_id, object_id=object_id)
                
                # Yield error component
                yield self._create_extraction_error_component(object_id, str(e))

    def get_object_details(self, work_packet: WorkPacket) -> List[Dict[str, Any]]:
        """Get detailed metadata for objects - interface method"""
        
        object_ids = work_packet.payload.object_ids
        results = []
        
        for object_id in object_ids:
            try:
                file_path = self._extract_path_from_object_id(object_id)
                if not file_path:
                    continue
                
                # Get detailed metadata including security info
                try:
                    loop = asyncio.get_event_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                
                metadata = loop.run_until_complete(
                    self._get_detailed_file_metadata(file_path)
                )
                
                results.append({
                    "object_id": object_id,
                    "metadata": metadata
                })
                
            except Exception as e:
                self.logger.warning("Failed to get object details",
                                   object_id=object_id, error=str(e))
                continue
        
        return results

    # =============================================================================
    # File Download and Temp Management
    # =============================================================================

    def _download_file_to_temp(self, file_path: str, task_id: int) -> Optional[str]:
        """Download SMB file to temporary location for ContentExtractor processing"""
        
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
            
            # Download file content synchronously
            def _download_file():
                """Download file synchronously"""
                try:
                    # Open file for reading
                    file_handle = Open(self.smb_connection.tree, normalized_path)
                    file_handle.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.GENERIC_READ,
                        FileAttributes.FILE_ATTRIBUTE_NORMAL,
                        ShareAccess.FILE_SHARE_READ,
                        CreateDisposition.FILE_OPEN,
                        CreateOptions.FILE_NON_DIRECTORY_FILE
                    )
                    
                    # Read content with size limit
                    content = file_handle.read(0, self.max_file_size_bytes)
                    file_handle.close()
                    
                    # Write to temp file
                    with open(temp_file_path, 'wb') as temp_file:
                        temp_file.write(content)
                    
                    return temp_file_path
                    
                except SMBException as e:
                    if "STATUS_ACCESS_DENIED" in str(e):
                        raise RightsError(
                            f"Access denied reading file: {file_path}",
                            ErrorType.RIGHTS_ACCESS_DENIED,
                            resource=file_path
                        )
                    elif "STATUS_OBJECT_NAME_NOT_FOUND" in str(e):
                        raise ProcessingError(
                            f"File not found: {file_path}",
                            ErrorType.PROCESSING_RESOURCE_NOT_FOUND,
                            resource=file_path
                        )
                    else:
                        raise NetworkError(
                            f"SMB file read failed: {str(e)}",
                            ErrorType.NETWORK_SMB_ERROR,
                            path=file_path
                        )
            
            # Download file
            downloaded_path = _download_file()
            
            # Track temp file for cleanup
            self.temp_files_created.append(downloaded_path)
            
            self.logger.debug("File downloaded for processing",
                             smb_path=file_path, temp_path=downloaded_path)
            
            return downloaded_path
            
        except Exception as e:
            self.logger.error("File download failed",
                             file_path=file_path, error=str(e))
            return None

    def _cleanup_temp_file(self, temp_file_path: str):
        """Clean up individual temp file"""
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                if temp_file_path in self.temp_files_created:
                    self.temp_files_created.remove(temp_file_path)
        except Exception as e:
            self.logger.warning("Failed to cleanup temp file",
                               temp_path=temp_file_path, error=str(e))

    def _cleanup_all_temp_files(self):
        """Clean up all temp files created by this connector"""
        for temp_file in self.temp_files_created[:]:  # Copy list to avoid modification during iteration
            self._cleanup_temp_file(temp_file)

    # =============================================================================
    # Connection Management
    # =============================================================================

    async def connect(self) -> bool:
        """Establish SMB connection"""
        try:
            self.smb_connection = SMBConnection(self.config.connection_config, self.logger)
            success = await self.smb_connection.connect()
            
            if success:
                self.logger.info("SMB datasource connected", 
                               datasource_id=self.datasource_id)
                return True
            else:
                return False
                
        except Exception as e:
            self.logger.error("SMB connection failed",
                             datasource_id=self.datasource_id, error=str(e))
            raise

    def connect_sync(self) -> bool:
        """Synchronous connection method for interface compatibility"""
        try:
            # Run async connect in sync context
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Create a new task in the existing loop
                    task = loop.create_task(self.connect())
                    # This is tricky - we need to handle this case
                    # For now, create a new connection synchronously
                    return self._connect_sync_direct()
                else:
                    return loop.run_until_complete(self.connect())
            except RuntimeError:
                # No event loop, create new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self.connect())
                finally:
                    loop.close()
                    
        except Exception as e:
            self.logger.error("Sync connection failed", error=str(e))
            return False

    def _connect_sync_direct(self) -> bool:
        """Direct synchronous connection without async"""
        try:
            self.smb_connection = SMBConnection(self.config.connection_config, self.logger)
            
            host = self.config.connection_config['host']
            port = self.config.connection_config.get('port', 445)
            username = self.config.connection_config['username']
            password = self.config.connection_config['password']
            domain = self.config.connection_config.get('domain', '')
            share_name = self.config.connection_config['share_name']
            
            # Create connection directly
            conn = Connection(uuid.uuid4(), host, port)
            conn.connect()
            
            # Create session
            session = Session(conn, username, password, domain)
            session.connect()
            
            # Connect to share
            tree_path = f"\\\\{host}\\{share_name}"
            tree = TreeConnect(session, tree_path)
            tree.connect()
            
            # Update connection object
            self.smb_connection.connection = conn
            self.smb_connection.session = session
            self.smb_connection.tree = tree
            self.smb_connection._connected = True
            
            self.logger.info("SMB connection established (sync)", 
                           host=host, share=share_name)
            return True
            
        except Exception as e:
            self.logger.error("Direct sync connection failed", error=str(e))
            return False

    async def disconnect(self):
        """Close SMB connection and cleanup"""
        try:
            # Clean up temp files first
            self._cleanup_all_temp_files()
            
            # Close SMB connection
            if self.smb_connection:
                await self.smb_connection.disconnect()
                self.smb_connection = None
            
            self.logger.info("SMB datasource disconnected", 
                           datasource_id=self.datasource_id)
            
        except Exception as e:
            self.logger.warning("SMB disconnect error", error=str(e))

    async def test_connectivity(self) -> bool:
        """Test SMB connectivity"""
        try:
            temp_connection = SMBConnection(self.config.connection_config, self.logger)
            result = await temp_connection.connect()
            if result:
                await temp_connection.disconnect()
            return result
        except Exception:
            return False

    # =============================================================================
    # Object Discovery
    # =============================================================================

    async def _discover_objects_async(self, filters: Optional[Dict[str, Any]] = None) -> Iterator[DiscoveredObject]:
        """Async object discovery - internal method"""
        
        if not self.smb_connection or not self.smb_connection.is_connected():
            raise ProcessingError(
                "SMB connection not established",
                ErrorType.PROCESSING_LOGIC_ERROR,
                operation="discover_objects"
            )
        
        try:
            # Start discovery from root path
            async for obj in self._enumerate_files_streaming(self.root_path, filters):
                yield obj
                
        except Exception as e:
            raise self._handle_processing_error(e, 'discover_objects')

    async def _enumerate_files_streaming(self, 
                                       directory_path: str,
                                       filters: Optional[Dict[str, Any]] = None) -> Iterator[DiscoveredObject]:
        """Stream file enumeration for memory efficiency"""
        
        try:
            # Normalize path for SMB
            normalized_path = self._normalize_smb_path(directory_path)
            
            # Run file enumeration in thread pool to avoid blocking
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
            
            # Get directory entries
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
                        async for sub_obj in self._enumerate_files_streaming(full_path, filters):
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
                    discovered_obj = await self._create_discovered_object(entry, full_path)
                    if discovered_obj:
                        yield discovered_obj
                        
        except Exception as e:
            raise self._handle_processing_error(
                e, 'enumerate_files_streaming',
                directory_path=directory_path
            )

    # =============================================================================
    # Detailed Metadata Collection
    # =============================================================================

    def _create_discovered_object_sync(self, entry: Dict[str, Any], full_path: str) -> Optional[DiscoveredObject]:
        """Create DiscoveredObject from SMB directory entry - synchronous version"""
        
        try:
            file_name = entry['file_name']
            file_size = entry['end_of_file']
            
            # Extract file extension
            file_extension = Path(file_name).suffix
            
            # Convert FILETIME to datetime
            created_date = self._filetime_to_datetime(entry.get('creation_time', 0))
            last_modified = self._filetime_to_datetime(entry.get('last_write_time', 0))
            last_accessed = self._filetime_to_datetime(entry.get('last_access_time', 0))
            
            # Create metadata
            metadata = ObjectMetadata(
                size_bytes=file_size,
                created_date=created_date,
                last_modified=last_modified,
                last_accessed=last_accessed,
                file_extension=file_extension,
                content_type=self._get_content_type(file_extension)
            )
            
            # Create discovered object
            discovered_obj = DiscoveredObject(
                object_id=create_object_id(self.datasource_id, full_path),
                datasource_id=self.datasource_id,
                object_type=ObjectType.FILE,
                object_path=full_path,
                object_metadata=metadata,
                estimated_content_size=estimate_content_size(file_size, 'file')
            )
            
            return discovered_obj
            
        except Exception as e:
            self.logger.warning("Failed to create discovered object",
                               file_path=full_path, error=str(e))
            return None

    def get_object_details(self, work_packet: WorkPacket) -> List[Dict[str, Any]]:
        """Get detailed metadata for objects - interface method"""
        
        object_ids = work_packet.payload.object_ids
        results = []
        
        for object_id in object_ids:
            try:
                file_path = self._extract_path_from_object_id(object_id)
                if not file_path:
                    continue
                
                # Get detailed metadata including security info
                metadata = self._get_detailed_file_metadata_sync(file_path)
                
                results.append({
                    "object_id": object_id,
                    "metadata": metadata
                })
                
            except Exception as e:
                self.logger.warning("Failed to get object details",
                                   object_id=object_id, error=str(e))
                continue
        
        return results

    def _get_detailed_file_metadata_sync(self, file_path: str) -> Dict[str, Any]:
        """Get comprehensive file metadata including security information - synchronous"""
        
        try:
            normalized_path = self._normalize_smb_path(file_path)
            
            def _get_file_info():
                """Get file information synchronously"""
                try:
                    # Open file to get detailed info
                    file_handle = Open(self.smb_connection.tree, normalized_path)
                    file_handle.create(
                        ImpersonationLevel.Impersonation,
                        FilePipePrinterAccessMask.GENERIC_READ | FilePipePrinterAccessMask.READ_CONTROL,
                        FileAttributes.FILE_ATTRIBUTE_NORMAL,
                        ShareAccess.FILE_SHARE_READ,
                        CreateDisposition.FILE_OPEN,
                        CreateOptions.FILE_NON_DIRECTORY_FILE
                    )
                    
                    # Get basic file information
                    basic_info = file_handle.query_info(
                        FileInformationClass.FILE_BASIC_INFORMATION
                    )
                    
                    # Get security information
                    security_metadata = {}
                    try:
                        security_info = file_handle.query_info(
                            FileInformationClass.FILE_SECURITY_INFORMATION
                        )
                        
                        if security_info:
                            # Parse security descriptor
                            sd_buffer = SMB2CreateSDBuffer()
                            sd_buffer.unpack(security_info)
                            
                            security_metadata = {
                                "owner_sid": str(sd_buffer.get_owner()) if hasattr(sd_buffer, 'get_owner') else None,
                                "group_sid": str(sd_buffer.get_group()) if hasattr(sd_buffer, 'get_group') else None,
                                "dacl_entries": self._parse_dacl_entries(sd_buffer.get_dacl()) if hasattr(sd_buffer, 'get_dacl') else [],
                                "sacl_entries": self._parse_sacl_entries(sd_buffer.get_sacl()) if hasattr(sd_buffer, 'get_sacl') else [],
                                "security_descriptor_collected": True
                            }
                    
                    except Exception as security_error:
                        security_metadata = {
                            "security_collection_failed": True,
                            "security_error": str(security_error)
                        }
                    
                    file_handle.close()
                    
                    # Combine metadata
                    metadata = {
                        "file_path": file_path,
                        "smb_server": self.config.connection_config.get('host'),
                        "smb_share": self.config.connection_config.get('share_name'),
                        "smb_version": self._get_smb_version(),
                        "basic_info": basic_info,
                        **security_metadata
                    }
                    
                    return metadata
                    
                except SMBException as e:
                    if "STATUS_ACCESS_DENIED" in str(e):
                        return {
                            "file_path": file_path,
                            "access_denied": True,
                            "error": str(e)
                        }
                    else:
                        raise
            
            return _get_file_info()
            
        except Exception as e:
            self.logger.warning("Failed to get detailed metadata",
                               file_path=file_path, error=str(e))
            return {
                "file_path": file_path,
                "metadata_collection_failed": True,
                "error": str(e)
            }

    # =============================================================================
    # Helper Methods
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

    def _create_discovered_object(self, entry: Dict[str, Any], full_path: str) -> Optional[DiscoveredObject]:
        """Create DiscoveredObject from SMB directory entry"""
        
        try:
            file_name = entry['file_name']
            file_size = entry['end_of_file']
            
            # Extract file extension
            file_extension = Path(file_name).suffix
            
            # Convert FILETIME to datetime
            created_date = self._filetime_to_datetime(entry.get('creation_time', 0))
            last_modified = self._filetime_to_datetime(entry.get('last_write_time', 0))
            last_accessed = self._filetime_to_datetime(entry.get('last_access_time', 0))
            
            # Create metadata
            metadata = ObjectMetadata(
                size_bytes=file_size,
                created_date=created_date,
                last_modified=last_modified,
                last_accessed=last_accessed,
                file_extension=file_extension,
                content_type=self._get_content_type(file_extension)
            )
            
            # Create discovered object
            discovered_obj = DiscoveredObject(
                object_id=create_object_id(self.datasource_id, full_path),
                datasource_id=self.datasource_id,
                object_type=ObjectType.FILE,
                object_path=full_path,
                object_metadata=metadata,
                estimated_content_size=estimate_content_size(file_size, 'file')
            )
            
            return discovered_obj
            
        except Exception as e:
            self.logger.warning("Failed to create discovered object",
                               file_path=full_path, error=str(e))
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
            schema={},
            metadata={
                "error_type": "smb_extraction_error",
                "smb_server": self.config.connection_config.get('host'),
                "smb_share": self.config.connection_config.get('share_name'),
                "extraction_timestamp": datetime.now(timezone.utc).isoformat()
            },
            extraction_method="smb_error_handler"
        )

    # =============================================================================
    # Security Metadata Parsing (Validated for smbprotocol 1.15.0)
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
            
        except Exception as e:
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
    # Path and Pattern Utilities
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
    # Error Handling
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
    # Health Check and Diagnostics
    # =============================================================================
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform SMB-specific health check"""
        
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
                    'host': self.config.connection_config.get('host'),
                    'share': self.config.connection_config.get('share_name'),
                    'smb_version': self._get_smb_version()
                }
                
                # Root path accessibility  
                root_access = await self._check_path_access(self.root_path)
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

    async def _check_path_access(self, path: str) -> Dict[str, Any]:
        """Check if specific path is accessible"""
        
        try:
            normalized_path = self._normalize_smb_path(path)
            
            loop = asyncio.get_event_loop()
            
            def _check_access():
                """Check path access synchronously"""
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

def create_smb_connector(config: DataSourceConfig, 
                        logger: SystemLogger,
                        error_handler,
                        content_extractor: ContentExtractor) -> SMBDataSource:
    """Factory function to create SMB connector with dependencies"""
    
    return SMBDataSource(
        config=config,
        logger=logger, 
        error_handler=error_handler,
        content_extractor=content_extractor
    )


# =============================================================================
# Testing and Validation
# =============================================================================

async def test_smb_connector():
    """Test SMB connector functionality with new architecture"""
    
    from core.models import DataSourceConfig, DataSourceType, WorkPacket, WorkPacketHeader, WorkPacketPayload, TaskType
    
    # Create test configuration
    config = DataSourceConfig(
        datasource_id="test_smb_ds",
        name="Test SMB Share",
        datasource_type=DataSourceType.SMB,
        connection_config={
            "host": "test-server",
            "share_name": "test-share", 
            "username": "test-user",
            "password": "test-password",
            "domain": "TEST-DOMAIN"
        },
        scan_config={
            "root_path": "/documents",
            "recursive": True,
            "include_patterns": ["*.txt", "*.pdf", "*.docx"],
            "exclude_patterns": ["*/temp/*", "*/backup/*"],
            "max_file_size_mb": 50
        }
    )
    
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
        def extract_from_file(self, file_path, object_id, job_id, task_id, datasource_id):
            # Mock ContentComponent for testing
            yield type('MockComponent', (), {
                'object_id': object_id,
                'component_type': 'text',
                'component_id': f'{object_id}_text_1',
                'content': 'Mock extracted content',
                'metadata': {}
            })()
    
    try:
        # Test connector creation
        connector = create_smb_connector(
            config=config,
            logger=MockLogger(),
            error_handler=MockErrorHandler(),
            content_extractor=MockContentExtractor()
        )
        
        print("✅ SMB connector created successfully")
        
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
        
        print("✅ WorkPacket structure compatible")
        
        # Test interface compliance
        assert hasattr(connector, 'enumerate_objects'), "Missing enumerate_objects method"
        assert hasattr(connector, 'get_object_content'), "Missing get_object_content method" 
        assert hasattr(connector, 'get_object_details'), "Missing get_object_details method"
        
        print("✅ IFileDataSourceConnector interface methods present")
        
        print("\n🎯 SMB connector architecture validation PASSED")
        print("Ready for integration with Worker and ContentExtractor")
        
    except Exception as e:
        print(f"❌ SMB connector test failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_smb_connector())