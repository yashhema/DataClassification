# Example: SMB Connector Integration with ContentExtractor
"""
Complete usage example showing how file-based connectors integrate
with the ContentExtractor module for content extraction and classification.
"""

from typing import Iterator, List
from core.models.models import WorkPacket, ContentComponent, ContentExtractionConfig
from core.interfaces.worker_interfaces import IFileDataSourceConnector
from core.logging.system_logger import SystemLogger
from core.config.configuration_manager import SystemConfig
from core.errors import ErrorHandler
from core.db.database_interface import DatabaseInterface
from content_extraction.content_extractor import ContentExtractor

class SMBConnector(IFileDataSourceConnector):
    """Example SMB connector using ContentExtractor for content processing"""
    
    def __init__(self, datasource_id: str, logger: SystemLogger,
                 error_handler: ErrorHandler, db_interface: DatabaseInterface,
                 system_config: SystemConfig):
        
        self.datasource_id = datasource_id
        self.logger = logger
        self.error_handler = error_handler
        self.db = db_interface
        self.system_config = system_config
        
        # Load datasource configuration
        self.datasource_config = self.db.get_datasource_configuration(datasource_id)
        
        # Load connector-specific configuration  
        self.connector_config = self.db.get_connector_configuration("smb", "default")
        
        # Parse content extraction config with defaults
        extraction_dict = self.datasource_config.configuration.get('content_extraction', {})
        self.extraction_config = ContentExtractionConfig.from_dict(extraction_dict)
        
        # Initialize shared ContentExtractor module
        self.content_extractor = ContentExtractor(
            extraction_config=self.extraction_config,
            system_config=self.system_config,
            logger=self.logger,
            error_handler=self.error_handler
        )
        
        # SMB-specific connection setup
        self.smb_connection = self._setup_smb_connection()

    def get_object_content(self, work_packet: WorkPacket) -> Iterator[ContentComponent]:
        """
        Implementation of IFileDataSourceConnector.get_object_content()
        Uses ContentExtractor for content processing
        """
        
        trace_id = work_packet.header.trace_id
        task_id = work_packet.header.task_id
        job_id = work_packet.header.job_id
        payload = work_packet.payload
        
        self.logger.log_database_operation(
            "GET_CONTENT", "SMB", "STARTED",
            trace_id=trace_id,
            task_id=task_id,
            object_count=len(payload.object_ids)
        )
        
        try:
            objects_processed = 0
            
            # Process each object serially (memory safe)
            for object_id in payload.object_ids:
                try:
                    # 1. SMB-specific: Get file path from object ID
                    file_info = self._get_file_info_from_object_id(object_id)
                    if not file_info:
                        self.logger.warning("Object not found", object_id=object_id)
                        continue
                    
                    # 2. SMB-specific: Download file to temp location
                    temp_file_path = self._download_file_to_temp(object_id, file_info, 
                                                               job_id, task_id, self.datasource_id)
                    
                    try:
                        # 3. SHARED: Use ContentExtractor for content processing
                        for component in self.content_extractor.extract_from_file(
                            file_path=temp_file_path,
                            object_id=object_id,
                            job_id=job_id,
                            task_id=task_id,
                            datasource_id=self.datasource_id
                        ):
                            # 4. Add SMB-specific metadata to components
                            component.metadata.update({
                                "smb_share": file_info.get("share_name"),
                                "smb_server": file_info.get("server_name"),
                                "original_file_path": file_info.get("file_path")
                            })
                            
                            yield component
                        
                    finally:
                        # 5. SMB-specific: Cleanup temp file
                        self._cleanup_temp_file(temp_file_path)
                    
                    objects_processed += 1
                    
                    # Send heartbeat periodically
                    if objects_processed % 10 == 0:
                        self._send_heartbeat(task_id)
                        
                except Exception as e:
                    # Log object-level error but continue processing
                    error = self.error_handler.handle_error(
                        e, f"smb_extract_content_{object_id}",
                        operation="file_content_extraction",
                        object_id=object_id,
                        trace_id=trace_id
                    )
                    self.logger.warning(f"Failed to extract content from {object_id}",
                                       error_id=error.error_id)
                    
                    # Yield error component and continue
                    yield self._create_connector_error_component(object_id, str(e))
                    continue
            
            # Log completion
            self.logger.log_database_operation(
                "GET_CONTENT", "SMB", "COMPLETED",
                trace_id=trace_id,
                task_id=task_id,
                objects_processed=objects_processed
            )
            
        except Exception as e:
            # System-level error
            error = self.error_handler.handle_error(
                e, "smb_get_object_content",
                operation="content_extraction_batch",
                datasource_id=self.datasource_id,
                trace_id=trace_id,
                task_id=task_id
            )
            self.logger.error("SMB content extraction failed", error_id=error.error_id)
            raise

    # =============================================================================
    # SMB-Specific Implementation Methods
    # =============================================================================

    def _get_file_info_from_object_id(self, object_id: str) -> dict:
        """SMB-specific: Get file information from object ID"""
        try:
            # Query database for object details
            discovered_objects = self.db.get_objects_for_classification([object_id])
            if not discovered_objects:
                return None
            
            discovered_obj = discovered_objects[0]
            
            return {
                "file_path": discovered_obj.ObjectPath,
                "share_name": self._extract_share_from_path(discovered_obj.ObjectPath),
                "server_name": self._extract_server_from_path(discovered_obj.ObjectPath),
                "size_bytes": discovered_obj.SizeBytes,
                "last_modified": discovered_obj.LastModified
            }
            
        except Exception as e:
            self.logger.error("Failed to get file info", object_id=object_id, error=str(e))
            return None

    def _download_file_to_temp(self, object_id: str, file_info: dict, 
                              job_id: int, task_id: int, datasource_id: str) -> str:
        """SMB-specific: Download file from SMB share to temp location"""
        try:
            # Create unique temp path using ContentExtractor pattern
            file_extension = os.path.splitext(file_info["file_path"])[1]
            temp_file_path = self.content_extractor._create_temp_path(
                job_id, task_id, datasource_id, f"smb_download{file_extension}"
            )
            
            # SMB-specific download logic
            smb_file_path = file_info["file_path"]
            
            with open(temp_file_path, 'wb') as temp_file:
                # Use SMB connection to download file
                file_data = self.smb_connection.read_file(smb_file_path)
                temp_file.write(file_data)
            
            self.logger.info("File downloaded successfully",
                           object_id=object_id,
                           temp_path=temp_file_path,
                           size_bytes=file_info["size_bytes"])
            
            return temp_file_path
            
        except Exception as e:
            self.logger.error("SMB file download failed",
                             object_id=object_id,
                             smb_path=file_info["file_path"],
                             error=str(e))
            raise

    def _cleanup_temp_file(self, temp_file_path: str):
        """SMB-specific: Clean up downloaded temp file"""
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                self.logger.debug("Temp file cleaned up", temp_file=temp_file_path)
        except Exception as e:
            self.logger.warning("Temp file cleanup failed", 
                               temp_file=temp_file_path, error=str(e))

    def _create_connector_error_component(self, object_id: str, error_message: str) -> ContentComponent:
        """Create SMB-specific error component"""
        return ContentComponent(
            object_id=object_id,
            component_type="extraction_error",
            component_id=f"{object_id}_smb_error_1",
            parent_path="",
            content=f"SMB connector error: {error_message}",
            original_size=0,
            extracted_size=len(error_message),
            is_truncated=False,
            schema={},
            metadata={
                "error_source": "smb_connector",
                "error": error_message,
                "datasource_id": self.datasource_id
            },
            extraction_method="smb_error_handler"
        )

# =============================================================================
# Usage Example: Worker Integration
# =============================================================================

class Worker:
    """Example showing how Worker uses file-based connectors with ContentExtractor"""
    
    def _process_file_based_classification(self, connector: IFileDataSourceConnector, 
                                         classifier, work_packet: WorkPacket):
        """Process CLASSIFICATION task for file-based connectors"""
        
        total_findings = 0
        components_processed = 0
        
        self.logger.info("Starting file-based classification",
                        task_id=work_packet.header.task_id,
                        object_count=len(work_packet.payload.object_ids))
        
        # Get ContentComponent stream from connector
        for content_component in connector.get_object_content(work_packet):
            try:
                # Route component to appropriate classification method
                findings = self._classify_content_component(content_component, classifier, work_packet)
                total_findings += len(findings)
                components_processed += 1
                
                self.logger.debug("Component classified",
                                component_id=content_component.component_id,
                                component_type=content_component.component_type,
                                findings_count=len(findings))
                
                # Send heartbeat periodically
                if components_processed % 50 == 0:
                    self._send_heartbeat(work_packet.header.task_id)
                    
            except Exception as e:
                self.logger.warning("Component classification failed",
                                   component_id=content_component.component_id,
                                   error=str(e))
                continue

        self.logger.info("File-based classification completed",
                        task_id=work_packet.header.task_id,
                        components_processed=components_processed,
                        total_findings=total_findings)

    def _classify_content_component(self, component: ContentComponent, 
                                  classifier, work_packet: WorkPacket) -> List:
        """Route ContentComponent to appropriate classification method"""
        
        try:
            if component.component_type == "table":
                # Route table components to row-by-row classification
                return self._classify_table_component(component, classifier, work_packet)
                
            elif component.component_type in ["text", "image_ocr", "archive_member"]:
                # Route text content to document classification
                return self._classify_text_component(component, classifier, work_packet)
                
            elif component.component_type == "table_fallback":
                # Treat table fallback as text content
                return self._classify_text_component(component, classifier, work_packet)
                
            elif component.component_type in ["extraction_error", "unsupported_format", 
                                            "file_too_large", "no_content_extractable"]:
                # Skip classification for error components
                self.logger.debug("Skipping classification for error component",
                                component_type=component.component_type)
                return []
                
            else:
                self.logger.warning("Unknown component type for classification",
                                   component_type=component.component_type,
                                   component_id=component.component_id)
                return []
                
        except Exception as e:
            self.logger.error("Component classification routing failed",
                             component_id=component.component_id,
                             error=str(e))
            return []

    def _classify_table_component(self, component: ContentComponent, 
                                classifier, work_packet: WorkPacket) -> List:
        """Classify table component using row-by-row processing"""
        
        findings = []
        
        try:
            # Parse table schema and rows
            table_schema = component.schema
            table_rows = table_schema.get("rows", [])
            
            # Build table metadata for classification
            table_metadata = {
                "table_name": component.component_id,
                "columns": table_schema.get("headers", []),
                "source_file": component.parent_path,
                "component_type": component.component_type,
                "extraction_method": component.extraction_method
            }
            
            # Add archive context if applicable
            if component.is_archive_extraction:
                table_metadata.update({
                    "archive_parent": component.metadata.get("archive_parent", ""),
                    "member_name": component.metadata.get("member_name", ""),
                    "extraction_source": "archive_member"
                })
            
            # Classify each row individually
            for row_index, row_data in enumerate(table_rows):
                row_pk = {"row_index": row_index, "component_id": component.component_id}
                
                # Use existing classification engine interface
                row_findings = classifier.classify_database_row(
                    row_data=row_data,
                    row_pk=row_pk,
                    table_metadata=table_metadata
                )
                
                # Add component context to findings
                for finding in row_findings:
                    if hasattr(finding, 'context_data'):
                        finding.context_data.update({
                            "component_id": component.component_id,
                            "component_type": component.component_type,
                            "extraction_method": component.extraction_method,
                            "is_archive_content": component.is_archive_extraction
                        })
                
                findings.extend(row_findings)
            
            self.logger.info("Table component classification completed",
                            component_id=component.component_id,
                            rows_processed=len(table_rows),
                            findings_found=len(findings))
            
            return findings
            
        except Exception as e:
            self.logger.error("Table component classification failed",
                             component_id=component.component_id,
                             error=str(e))
            return []

    def _classify_text_component(self, component: ContentComponent,
                               classifier, work_packet: WorkPacket) -> List:
        """Classify text component using document content processing"""
        
        try:
            # Build file metadata for classification
            file_metadata = {
                "file_path": component.parent_path,
                "file_name": os.path.basename(component.parent_path),
                "component_id": component.component_id,
                "component_type": component.component_type,
                "extraction_method": component.extraction_method
            }
            
            # Add archive context if applicable
            if component.is_archive_extraction:
                file_metadata.update({
                    "extraction_source": "archive_member",
                    "archive_parent": component.metadata.get("archive_parent", ""),
                    "member_name": component.metadata.get("member_name", "")
                })
            else:
                file_metadata["extraction_source"] = "file"
            
            # Use existing classification engine interface
            findings = classifier.classify_document_content(
                content=component.content,
                file_metadata=file_metadata
            )
            
            # Add component context to findings
            for finding in findings:
                if hasattr(finding, 'context_data'):
                    finding.context_data.update({
                        "component_id": component.component_id,
                        "component_type": component.component_type,
                        "extraction_method": component.extraction_method,
                        "is_archive_content": component.is_archive_extraction
                    })
            
            self.logger.info("Text component classification completed",
                            component_id=component.component_id,
                            content_length=len(component.content),
                            findings_found=len(findings))
            
            return findings
            
        except Exception as e:
            self.logger.error("Text component classification failed",
                             component_id=component.component_id,
                             error=str(e))
            return []

# =============================================================================
# Configuration Setup Example
# =============================================================================

def setup_smb_connector_with_content_extraction():
    """Example: Setting up SMB connector with content extraction configuration"""
    
    # 1. Define datasource content extraction configuration
    datasource_content_config = {
        "limits": {
            "max_file_size_mb": 50,
            "max_component_size_mb": 10,
            "max_text_chars": 500000,
            "max_document_table_rows": 5000,
            "max_archive_members": 100,
            "max_archive_depth": 3,
            "sampling_strategy": "head"
        },
        "features": {
            "extract_tables": True,
            "extract_pictures": True,
            "extract_archives": True,
            "ocr_enabled": True,
            "preserve_structure": True,
            "include_metadata": True
        }
    }
    
    # 2. Update datasource configuration in database
    datasource_config = {
        "connection_config": {
            "server": "smb-server.company.com",
            "share": "documents",
            "username": "scanner_user",
            "domain": "company"
        },
        "scan_config": {
            "paths": ["/finance", "/hr", "/legal"],
            "file_extensions": ["pdf", "docx", "xlsx", "pptx"]
        },
        "content_extraction": datasource_content_config  # NEW: Content extraction config
    }
    
    # 3. Add system-level temp directory configuration
    # Add to configuration_manager.py SystemIdentityConfig:
    system_config_addition = {
        "temp_extraction_directory": "/data/temp/extraction"  # Custom temp path with more space
    }
    
    return datasource_config, system_config_addition

# =============================================================================
# Example: Processing Different File Types
# =============================================================================

def example_content_extraction_flow():
    """Example showing ContentExtractor processing different file types"""
    
    # Sample files and expected components
    test_scenarios = [
        {
            "file": "financial_report.pdf",
            "expected_components": [
                {"type": "text", "id": "financial_report_pdf_text_1"},
                {"type": "table", "id": "financial_report_pdf_table_1"},
                {"type": "table", "id": "financial_report_pdf_table_2"},
                {"type": "image_ocr", "id": "financial_report_pdf_image_ocr_1"}
            ]
        },
        {
            "file": "contracts.zip",
            "expected_components": [
                {"type": "archive_member", "id": "contract1_pdf_text_1", "parent": "contracts.zip/contract1.pdf"},
                {"type": "archive_member", "id": "contract1_pdf_table_1", "parent": "contracts.zip/contract1.pdf"},
                {"type": "archive_member", "id": "budget_xlsx_sheet_1", "parent": "contracts.zip/budget.xlsx"}
            ]
        },
        {
            "file": "presentation.pptx",
            "expected_components": [
                {"type": "text", "id": "presentation_pptx_text_1"},
                {"type": "table", "id": "presentation_pptx_table_1"},
                {"type": "image_ocr", "id": "presentation_pptx_image_ocr_1"}
            ]
        }
    ]
    
    return test_scenarios

# =============================================================================
# Example: Component Processing Results
# =============================================================================

def example_component_output():
    """Example showing actual ContentComponent output structure"""
    
    # Example: PDF text component
    pdf_text_component = ContentComponent(
        object_id="obj_12345",
        component_type="text",
        component_id="financial_report_pdf_text_1",
        parent_path="/smb/finance/financial_report.pdf",
        content="Q4 Financial Summary\nRevenue: $1.2M\nExpenses: $800K...",
        original_size=15420,
        extracted_size=15420,
        is_truncated=False,
        schema={},
        metadata={
            "component_type": "text",
            "extraction_timestamp": "2025-09-02T15:30:00Z",
            "extraction_config_version": "1.0",
            "page_count": 12,
            "total_chars": 15420,
            "smb_share": "finance",
            "smb_server": "smb-server.company.com"
        },
        extraction_method="pymupdf_text",
        is_archive_extraction=False
    )
    
    # Example: Archive member table component
    archive_table_component = ContentComponent(
        object_id="obj_12346/contract1.pdf",
        component_type="archive_member",  # Key difference for archive content
        component_id="contract1_pdf_table_1",
        parent_path="contracts.zip/contract1.pdf",
        content="",  # Empty for tables
        original_size=2048,
        extracted_size=0,
        is_truncated=False,
        schema={
            "headers": ["Item", "Quantity", "Price"],
            "rows": [
                {"Item": "Consulting", "Quantity": "40", "Price": "$5000"},
                {"Item": "Training", "Quantity": "20", "Price": "$2000"}
            ],
            "row_count": 2,
            "column_count": 3,
            "serialized_rows": "Consulting|40|$5000\nTraining|20|$2000"
        },
        metadata={
            "component_type": "archive_member",
            "extraction_timestamp": "2025-09-02T15:30:00Z",
            "original_component_type": "table",  # Preserves original type
            "archive_parent": "contracts.zip",
            "member_name": "contract1.pdf",
            "page_number": 1
        },
        extraction_method="pymupdf_tables",
        is_archive_extraction=True
    )
    
    return [pdf_text_component, archive_table_component]

# =============================================================================
# Integration Benefits
# =============================================================================

"""
BENEFITS OF CONTENTEXTRACTOR INTEGRATION:

1. CODE REUSE:
   - Same extraction logic across all file-based connectors (SMB, S3, Azure)
   - No duplication of PDF, Word, Excel processing code
   - Consistent error handling and component structure

2. CONSISTENCY:
   - Identical component format across all file-based connectors
   - Standardized metadata structure
   - Unified error handling patterns

3. MAINTENANCE:
   - Single place to update extraction algorithms
   - Centralized configuration management
   - Single testing suite for all extraction logic

4. CONNECTOR SPECIALIZATION:
   - SMB Connector: Handles SMB authentication, file download, path resolution
   - S3 Connector: Handles AWS credentials, bucket access, object metadata
   - Azure Connector: Handles Azure auth, blob storage, container management
   - ContentExtractor: Handles file content processing universally

5. CLASSIFICATION INTEGRATION:
   - Components route directly to existing EngineInterface methods
   - Table components → classify_database_row()
   - Text components → classify_document_content()
   - Archive members treated as specialized content with preserved context
"""