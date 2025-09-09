#!/usr/bin/env python3
"""
Content Extractor Test Suite

Comprehensive test cases for validating the ContentExtractor implementation.
Tests cover all file types, features, edge cases, and security scenarios.
"""

import pytest
import asyncio
import tempfile
import os
import json
import zipfile
import tarfile
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock
from typing import List, Dict, Any

# Import the system under test - adjust these imports to match your actual module structure

try:
    from content_extraction.content_extractor import ContentExtractor
    from core.models.models import ContentComponent, ContentExtractionConfig
    from core.logging.system_logger import SystemLogger
    from core.config.configuration_manager import SystemConfig
    from core.errors import ErrorHandler
    IMPORTS_AVAILABLE = True
except ImportError as e:
    IMPORTS_AVAILABLE = False
    print(f"Import error: {e}")
    print("Please adjust the import paths to match your project structure")
    
    # Mock the classes for testing the test structure itself
    from unittest.mock import Mock
    
    class ContentComponent:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
    
    class ContentExtractionConfig:
        def __init__(self):
            self.limits = Mock()
            self.features = Mock()
            
    ContentExtractor = Mock
    SystemLogger = Mock  
    SystemConfig = Mock
    ErrorHandler = Mock

class TestContentExtractor:
    """Main test class for ContentExtractor"""
    
    @pytest.fixture
    def mock_system_config(self):
        """Mock system configuration - Fixed"""
        if not IMPORTS_AVAILABLE:
            pytest.skip("Content Extractor modules not available")
            
        # Create a regular Mock instead of spec'd mock to allow attribute access
        config = Mock()
        
        # Create nested mock objects for the hierarchical structure
        config.system = Mock()
        config.system.total_process_memory_limit_mb = 1024
        config.system.temp_extraction_directory = tempfile.gettempdir()
        
        config.connector = Mock()
        config.connector.easyocr_support_enabled = True
        
        return config



    @pytest.fixture
    def extraction_config(self):
        """Standard extraction configuration for testing"""
        config = ContentExtractionConfig()
        config.limits.max_file_size_mb = 100
        config.limits.max_component_size_mb = 10
        config.limits.max_text_chars = 50000
        config.limits.max_archive_depth = 3
        config.limits.max_archive_members = 50
        config.limits.max_document_table_rows = 100
        config.limits.sampling_strategy = "head"
        
        config.features.extract_tables = True
        config.features.extract_pictures = True
        config.features.ocr_enabled = True
        
        return config
    
    @pytest.fixture
    def mock_logger(self):
        """Mock system logger"""
        return Mock(spec=SystemLogger)
    
    @pytest.fixture
    def mock_error_handler(self):
        """Mock error handler"""
        return Mock(spec=ErrorHandler)
    
    @pytest.fixture
    def content_extractor(self, extraction_config, mock_system_config, 
                         mock_logger, mock_error_handler):
        """Create ContentExtractor instance for testing"""
        return ContentExtractor(
            extraction_config=extraction_config,
            system_config=mock_system_config,
            logger=mock_logger,
            error_handler=mock_error_handler
        )
    
    @pytest.fixture
    def test_files_dir(self):
        """Path to test files directory - configurable via environment or default"""
        # Method 1: Environment variable
        test_dir = os.environ.get("CONTENT_EXTRACTOR_TEST_FILES", "test_files")
        
        # Method 2: Relative to test file location
        # test_dir = Path(__file__).parent / "test_files"
        
        # Method 3: Absolute path option
        # test_dir = os.environ.get("CONTENT_EXTRACTOR_TEST_FILES", 
        #                          "/absolute/path/to/test_files")
        
        test_path = Path(test_dir)
        
        # Verify test directory exists
        if not test_path.exists():
            pytest.skip(f"Test files directory not found: {test_path}")
        
        return test_path

class TestBasicFileTypes(TestContentExtractor):
    """Test basic file type extraction"""
    
    @pytest.mark.skipif(not IMPORTS_AVAILABLE, reason="Content Extractor modules not available")
    @pytest.mark.asyncio
    async def test_text_file_extraction(self, content_extractor, test_files_dir):
        """Test plain text file extraction"""
        text_file = test_files_dir / "basic_types" / "sample.txt"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(text_file), "test_object_1", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) == 1
        component = components[0]
        assert component.component_type == "text"
        assert component.extraction_method == "text_file_read"
        assert "Sample Text Document" in component.content
        assert component.metadata["encoding"] == "utf-8"
    @pytest.mark.asyncio
    async def test_image_ocr_processing(self, content_extractor, test_files_dir):
        """Test OCR text extraction from images"""
        image_file = test_files_dir / "basic_types" / "sample.png"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(image_file), "test_ocr", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) == 1
        assert components[0].component_type == "image_ocr"
        assert len(components[0].content) > 0  # Should have extracted text

    @pytest.mark.asyncio
    async def test_json_file_extraction(self, content_extractor, test_files_dir):
        """Test JSON file extraction and parsing"""
        json_file = test_files_dir / "basic_types" / "sample.json"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(json_file), "test_object_2", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) == 1
        component = components[0]
        assert component.component_type == "text"
        assert component.extraction_method == "json_parsing"
        assert "john.doe@example.com" in component.content
        assert component.metadata["json_type"] in ["dict", "list"]
    
    @pytest.mark.asyncio
    async def test_xml_file_extraction(self, content_extractor, test_files_dir):
        """Test XML file extraction with potential table detection"""
        xml_file = test_files_dir / "basic_types" / "sample.xml"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(xml_file), "test_object_3", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should have at least text component, possibly table component
        assert len(components) >= 1
        
        text_components = [c for c in components if c.component_type == "text"]
        assert len(text_components) == 1
        assert "Sample XML Document" in text_components[0].content
        assert text_components[0].extraction_method == "xml_parsing"
    
    @pytest.mark.asyncio
    async def test_html_file_extraction(self, content_extractor, test_files_dir):
        """Test HTML file extraction with table detection"""
        html_file = test_files_dir / "basic_types" / "sample.html"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(html_file), "test_object_4", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should have text component and potentially table/image components
        component_types = [c.component_type for c in components]
        assert "text" in component_types
        
        text_component = next(c for c in components if c.component_type == "text")
        assert "Sample HTML Content" in text_component.content
        assert text_component.extraction_method == "beautifulsoup_text"
    
    @pytest.mark.asyncio
    async def test_email_file_extraction(self, content_extractor, test_files_dir):
        """Test email file extraction"""
        email_file = test_files_dir / "basic_types" / "sample.eml"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(email_file), "test_object_5", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) >= 1
        component = components[0]
        assert component.component_type == "text"
        assert component.extraction_method == "email_parsing"
        assert "sender@example.com" in component.content
        assert "Confidential Employee Information" in component.content

class TestArchiveProcessing(TestContentExtractor):
    """Test archive processing capabilities"""
    
    @pytest.mark.asyncio
    async def test_simple_zip_processing(self, content_extractor, test_files_dir):
        """Test basic ZIP archive processing"""
        zip_file = test_files_dir / "archives" / "simple.zip"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(zip_file), "test_archive_1", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should have components for each archive member
        assert len(components) >= 3  # readme.txt, data.json, report.html
        
        # All components should be marked as archive members
        for component in components:
            assert component.component_type == "archive_member"
            assert component.is_archive_extraction == True
            assert "archive_parent" in component.metadata
            assert "member_name" in component.metadata
    
    @pytest.mark.asyncio
    async def test_nested_zip_processing(self, content_extractor, test_files_dir):
        """Test nested archive processing with depth control"""
        nested_zip = test_files_dir / "archives" / "nested.zip"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(nested_zip), "test_nested_1", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should process both outer and inner archive contents
        member_names = [comp.metadata.get("member_name", "") for comp in components]
        
        # Check for outer file
        assert any("outer_file.txt" in name for name in member_names)
        
        # Check for inner archive processing
        nested_components = [c for c in components if "nested_data.txt" in c.metadata.get("member_name", "")]
        assert len(nested_components) > 0
    
    @pytest.mark.asyncio
    async def test_tar_archive_processing(self, content_extractor, test_files_dir):
        """Test TAR archive processing"""
        tar_file = test_files_dir / "archives" / "sample.tar.gz"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(tar_file), "test_tar_1", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Verify TAR members are processed
        assert len(components) >= 2  # archive_text.txt, archive_data.json
        
        for component in components:
            assert component.component_type == "archive_member"
            assert component.extraction_method in ["text_file_read", "json_parsing"]
    
    @pytest.mark.asyncio
    async def test_archive_depth_limiting(self, content_extractor):
        """Test archive depth limiting prevents infinite recursion"""
        # Set low depth limit
        content_extractor.extraction_config.limits.max_archive_depth = 1
        
        # Create deeply nested archive for testing
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            with zipfile.ZipFile(temp_zip_path, 'w') as outer_zip:
                # Create inner ZIP
                from io import BytesIO
                inner_buffer = BytesIO()
                with zipfile.ZipFile(inner_buffer, 'w') as inner_zip:
                    # Create even deeper ZIP
                    deeper_buffer = BytesIO()
                    with zipfile.ZipFile(deeper_buffer, 'w') as deeper_zip:
                        deeper_zip.writestr("deep_file.txt", "Deep content")
                    inner_zip.writestr("deeper.zip", deeper_buffer.getvalue())
                
                outer_zip.writestr("inner.zip", inner_buffer.getvalue())
            
            components = []
            async for component in content_extractor.extract_from_file(
                temp_zip_path, "test_depth", 1, 1, "test_ds"
            ):
                components.append(component)
            
            # Should have depth exceeded error
            error_components = [c for c in components if c.component_type == "extraction_error"]
            assert len(error_components) > 0
            assert any("depth" in c.content.lower() for c in error_components)
            
        finally:
            os.unlink(temp_zip_path)
    
    @pytest.mark.asyncio
    async def test_circular_reference_detection(self, content_extractor):
        """Test circular reference detection in archives"""
        # This test simulates the scenario - actual circular ZIP creation is complex
        
        # Manually trigger circular reference detection
        content_extractor.processed_archive_paths.add("/fake/archive.zip")
        
        components = []
        with patch.object(content_extractor, '_process_zip_archive') as mock_zip:
            # Simulate processing the same archive again
            mock_zip.return_value = content_extractor._create_circular_reference_component(
                "test_circular", "/fake/archive.zip"
            )
            
            async for component in content_extractor._process_archive(
                "/fake/archive.zip", "test_circular", "zip", 1, 1, "test_ds"
            ):
                components.append(component)
        
        assert len(components) == 1
        assert components[0].component_type == "extraction_error"
        assert "circular" in components[0].content.lower()

class TestResourceLimits(TestContentExtractor):
    """Test resource limit enforcement"""
    
    @pytest.mark.asyncio
    async def test_oversized_file_handling(self, content_extractor):
        """Test handling of files exceeding size limits"""
        # Set very low file size limit for testing
        content_extractor.extraction_config.limits.max_file_size_mb = 0.001  # 1KB
        
        # Create file larger than limit
        large_content = "x" * 2000  # 2KB
        with tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False) as temp_file:
            temp_file.write(large_content)
            temp_path = temp_file.name
        
        try:
            components = []
            async for component in content_extractor.extract_from_file(
                temp_path, "test_oversized", 1, 1, "test_ds"
            ):
                components.append(component)
            
            assert len(components) == 1
            assert components[0].component_type == "file_too_large"
            assert "exceeds limit" in components[0].content
            
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.asyncio
    async def test_component_size_limiting(self, content_extractor):
        """Test component content truncation"""
        # Set low text character limit
        content_extractor.extraction_config.limits.max_text_chars = 100
        
        # Create file with content exceeding limit
        large_content = "This is test content. " * 20  # > 100 chars
        with tempfile.NamedTemporaryFile(mode='w', suffix=".txt", delete=False) as temp_file:
            temp_file.write(large_content)
            temp_path = temp_file.name
        
        try:
            components = []
            async for component in content_extractor.extract_from_file(
                temp_path, "test_truncation", 1, 1, "test_ds"
            ):
                components.append(component)
            
            assert len(components) == 1
            component = components[0]
            assert len(component.content) <= 100
            # Note: is_truncated logic may need adjustment based on actual implementation
            
        finally:
            os.unlink(temp_path)
    
    @pytest.mark.asyncio 
    async def test_archive_member_count_limit(self, content_extractor):
        """Test archive member count limiting"""
        # Set low member count limit
        content_extractor.extraction_config.limits.max_archive_members = 3
        
        # Create ZIP with many files
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            with zipfile.ZipFile(temp_zip_path, 'w') as zf:
                # Add more files than the limit
                for i in range(10):
                    zf.writestr(f"file_{i}.txt", f"Content {i}")
            
            components = []
            async for component in content_extractor.extract_from_file(
                temp_zip_path, "test_member_limit", 1, 1, "test_ds"
            ):
                components.append(component)
            
            # Should only process up to the limit
            assert len(components) <= 3
            
        finally:
            os.unlink(temp_zip_path)

class TestFeatureToggles(TestContentExtractor):
    """Test feature toggle functionality"""

    @pytest.mark.asyncio
    async def test_ocr_disabled_system_level_fixed(self, extraction_config, mock_logger, mock_error_handler):
        """Test OCR disabled at system level - Fixed"""
        # Mock system config with OCR disabled
        system_config = Mock()
        system_config.system = Mock()
        system_config.system.total_process_memory_limit_mb = 1024
        system_config.system.temp_extraction_directory = tempfile.gettempdir()
        
        system_config.connector = Mock()
        system_config.connector.easyocr_support_enabled = False  # Disabled

        extractor = ContentExtractor(extraction_config, system_config, mock_logger, mock_error_handler)

        # Create a proper image file (minimal PNG)
        # Minimal PNG file header
        png_data = (
            b'\x89PNG\r\n\x1a\n'  # PNG signature
            b'\x00\x00\x00\rIHDR'  # IHDR chunk
            b'\x00\x00\x00\x01'    # Width: 1
            b'\x00\x00\x00\x01'    # Height: 1
            b'\x08\x02'            # Bit depth: 8, Color type: 2 (RGB)
            b'\x00\x00\x00'        # Compression, filter, interlace
            b'\x90wS\xde'          # CRC
            b'\x00\x00\x00\x0cIDATx\x9cc\xf8\x00\x00\x00\x01\x00\x01\x90wS\xde'  # Image data
            b'\x00\x00\x00\x00IEND\xaeB`\x82'  # IEND chunk
        )

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as temp_img:
            temp_img.write(png_data)
            temp_path = temp_img.name

        try:
            components = []
            async for component in extractor.extract_from_file(
                temp_path, "test_no_ocr", 1, 1, "test_ds"
            ):
                components.append(component)

            assert len(components) == 1
            assert components[0].component_type == "no_content_extractable"
            assert "OCR not available" in components[0].content
            
        finally:
            os.unlink(temp_path)


    @pytest.mark.asyncio
    async def test_tables_disabled(self, content_extractor, test_files_dir):
        """Test table extraction disabled"""
        content_extractor.extraction_config.features.extract_tables = False
        
        html_file = test_files_dir / "complex_content" / "tables.html"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(html_file), "test_no_tables", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should only have text component, no table components
        component_types = [c.component_type for c in components]
        assert "table" not in component_types
        assert "text" in component_types

class TestComplexContent(TestContentExtractor):
    """Test complex content extraction scenarios"""
    
    @pytest.mark.asyncio
    async def test_html_with_tables(self, content_extractor, test_files_dir):
        """Test HTML file with multiple tables"""
        tables_html = test_files_dir / "complex_content" / "tables.html"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(tables_html), "test_html_tables", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should have text component and table components
        text_components = [c for c in components if c.component_type == "text"]
        table_components = [c for c in components if c.component_type == "table"]
        
        assert len(text_components) >= 1
        assert len(table_components) >= 1  # HTML has tables
        
        # Verify table structure
        for table_comp in table_components:
            assert "headers" in table_comp.schema
            assert "rows" in table_comp.schema
            assert table_comp.extraction_method == "beautifulsoup_tables"
    
    @pytest.mark.asyncio
    async def test_xml_table_extraction(self, content_extractor, test_files_dir):
        """Test table extraction from XML with repeating elements"""
        employees_xml = test_files_dir / "complex_content" / "employees.xml"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(employees_xml), "test_xml_tables", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Look for table component from repeating XML elements
        table_components = [c for c in components if c.component_type == "table"]
        
        if table_components:  # XML table detection is heuristic
            table_comp = table_components[0]
            assert "headers" in table_comp.schema
            assert len(table_comp.schema["rows"]) >= 3  # 3 employees
    
    @pytest.mark.asyncio
    async def test_email_with_html_tables(self, content_extractor, test_files_dir):
        """Test email containing HTML with tables"""
        html_email = test_files_dir / "complex_content" / "html_email.eml"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(html_email), "test_email_tables", 1, 1, "test_ds"
        ):
            components.append(component)
        
        component_types = [c.component_type for c in components]
        
        # Should extract both email content and tables from HTML part
        assert "text" in component_types  # Email text
        # Table extraction depends on email parser handling HTML parts

class TestErrorHandling(TestContentExtractor):
    """Test error handling and edge cases"""
    
    @pytest.mark.asyncio
    async def test_nonexistent_file(self, content_extractor):
        """Test handling of non-existent files"""
        fake_path = "/path/that/does/not/exist.txt"
        
        components = []
        async for component in content_extractor.extract_from_file(
            fake_path, "test_missing", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) == 1
        assert components[0].component_type == "extraction_error"
        assert "not found" in components[0].content.lower()
    
    @pytest.mark.asyncio
    async def test_corrupted_json(self, content_extractor, test_files_dir):
        """Test handling of malformed JSON"""
        malformed_json = test_files_dir / "edge_cases" / "malformed.json"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(malformed_json), "test_bad_json", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) == 1
        assert components[0].component_type == "extraction_error"
        assert "JSON extraction failed" in components[0].content
    
    @pytest.mark.asyncio
    async def test_corrupted_xml(self, content_extractor, test_files_dir):
        """Test handling of malformed XML"""
        malformed_xml = test_files_dir / "edge_cases" / "malformed.xml"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(malformed_xml), "test_bad_xml", 1, 1, "test_ds"
        ):
            components.append(component)
        
        assert len(components) == 1
        assert components[0].component_type == "extraction_error"
        assert "XML extraction failed" in components[0].content
    
    @pytest.mark.asyncio
    async def test_empty_files(self, content_extractor, test_files_dir):
        """Test handling of empty files"""
        empty_txt = test_files_dir / "edge_cases" / "empty.txt"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(empty_txt), "test_empty", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Empty file should still create a component (empty content)
        assert len(components) == 1
        assert components[0].component_type == "text"
        assert components[0].content == ""

class TestSecurityValidation(TestContentExtractor):
    """Test security features and attack prevention"""
    
    @pytest.mark.asyncio
    async def test_path_traversal_prevention(self, content_extractor, test_files_dir):
        """Test prevention of path traversal attacks in ZIP files"""
        malicious_zip = test_files_dir / "security_tests" / "path_traversal.zip"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(malicious_zip), "test_path_traversal", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Malicious paths should be skipped, only safe files processed
        member_names = [comp.metadata.get("member_name", "") for comp in components]
        
        # Should not contain any path traversal attempts
        for name in member_names:
            assert "../" not in name
            assert not name.startswith("/")
            assert ":" not in name or not (len(name) > 1 and name[1] == ":")
    
    @pytest.mark.asyncio
    async def test_archive_member_limit(self, content_extractor, test_files_dir):
        """Test archive member count limiting"""
        many_files_zip = test_files_dir / "security_tests" / "many_files.zip"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(many_files_zip), "test_member_limit", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should respect member count limit
        max_members = content_extractor.extraction_config.limits.max_archive_members
        assert len(components) <= max_members

class TestTableExtraction(TestContentExtractor):
    """Test table extraction from various sources"""
    
    def test_html_table_conversion(self, content_extractor):
        """Test HTML table to structured data conversion"""
        from bs4 import BeautifulSoup
        
        html_table = """
        <table>
            <tr><th>Name</th><th>SSN</th><th>Department</th></tr>
            <tr><td>John Doe</td><td>123-45-6789</td><td>IT</td></tr>
            <tr><td>Jane Smith</td><td>987-65-4321</td><td>HR</td></tr>
        </table>
        """
        
        soup = BeautifulSoup(html_table, 'html.parser')
        table = soup.find('table')
        
        result = content_extractor._convert_html_table(table)
        
        assert result["headers"] == ["Name", "SSN", "Department"]
        assert len(result["rows"]) == 2
        assert result["rows"][0]["Name"] == "John Doe"
        assert result["rows"][0]["SSN"] == "123-45-6789"
        assert "123-45-6789" in result["serialized_rows"]
    
    def test_xml_table_detection(self, content_extractor):
        """Test table detection in XML with repeating elements"""
        import xml.etree.ElementTree as ET
        
        # Create XML with repeating structure
        xml_content = """
        <employees>
            <employee><name>John</name><id>1</id><dept>IT</dept></employee>
            <employee><name>Jane</name><id>2</id><dept>HR</dept></employee>
            <employee><name>Bob</name><id>3</id><dept>Finance</dept></employee>
        </employees>
        """
        
        root = ET.fromstring(xml_content)
        result = content_extractor._extract_xml_tables(root)
        
        assert result is not None
        assert result["headers"] == ["name", "id", "dept"]
        assert len(result["rows"]) == 3
        assert result["rows"][0]["name"] == "John"

class TestFileTypeDetection(TestContentExtractor):
    """Test file type detection accuracy"""
    
    def test_extension_based_detection(self, content_extractor):
        """Test file type detection by extension"""
        
        test_cases = [
            ("/path/document.pdf", "pdf"),
            ("/path/spreadsheet.xlsx", "xlsx"),
            ("/path/presentation.pptx", "pptx"),
            ("/path/document.docx", "docx"),
            ("/path/webpage.html", "html"),
            ("/path/data.json", "json"),
            ("/path/config.xml", "xml"),
            ("/path/archive.zip", "zip"),
            ("/path/backup.tar.gz", "tar"),
            ("/path/image.jpg", "image"),
            ("/path/email.eml", "eml"),
            ("/path/unknown.xyz", "unsupported")
        ]
        
        for file_path, expected_type in test_cases:
            detected_type = content_extractor._detect_type_by_extension(file_path)
            assert detected_type == expected_type, f"Failed for {file_path}"
    
    @pytest.mark.asyncio
    async def test_wrong_extension_handling(self, content_extractor, test_files_dir):
        """Test files with misleading extensions"""
        wrong_ext_file = test_files_dir / "edge_cases" / "json_as_txt.txt"
        
        components = []
        async for component in content_extractor.extract_from_file(
            str(wrong_ext_file), "test_wrong_ext", 1, 1, "test_ds"
        ):
            components.append(component)
        
        # Should process as text file (based on extension)
        assert len(components) == 1
        assert components[0].component_type == "text"
        assert components[0].extraction_method == "text_file_read"

class TestConfigurationValidation(TestContentExtractor):
    """Test configuration validation and warnings"""
    
    def test_config_conflict_detection(self, extraction_config, mock_logger, mock_error_handler):
        """Test detection of configuration conflicts"""
        # Set conflicting OCR settings
        system_config = Mock()
        system_config.system.total_process_memory_limit_mb = 1024
        system_config.connector.easyocr_support_enabled = False  # System disabled
        
        extraction_config.features.ocr_enabled = True  # Datasource enabled
        
        # Should log validation warning
        extractor = ContentExtractor(extraction_config, system_config, mock_logger, mock_error_handler)
        
        # Verify OCR is actually disabled
        assert not extractor._is_ocr_enabled()
    
    def test_memory_limit_validation(self, extraction_config, mock_logger, mock_error_handler):
        """Test memory limit validation"""
        system_config = Mock()
        system_config.system.total_process_memory_limit_mb = 100  # Low limit
        
        extraction_config.limits.max_file_size_mb = 200  # Exceeds safe limit
        
        # Should create extractor but log warning
        extractor = ContentExtractor(extraction_config, system_config, mock_logger, mock_error_handler)
        
        # Verify resource validation fails for large files
        assert not extractor._validate_extraction_resources(150)  # 150MB file

class TestComponentGeneration(TestContentExtractor):
    """Test component creation and metadata generation"""
    
    def test_component_id_generation(self, content_extractor):
        """Test unique component ID generation"""
        
        # Test various file paths
        test_cases = [
            ("/path/document.pdf", "document"),
            ("/path/data file.xlsx", "data file"),
            ("simple.txt", "simple"),
            ("/complex/path/file.name.ext.txt", "file")
        ]
        
        for file_path, expected_base in test_cases:
            base_name = content_extractor._get_base_name(file_path)
            assert base_name == expected_base
    
    def test_metadata_generation(self, content_extractor):
        """Test standard metadata generation"""
        
        metadata = content_extractor._get_standard_metadata("test_type", {"custom": "value"})
        
        assert metadata["component_type"] == "test_type"
        assert metadata["custom"] == "value"
        assert "extraction_timestamp" in metadata
        assert "extraction_config_version" in metadata
    
    def test_error_component_creation(self, content_extractor):
        """Test error component standardization"""
        
        error_comp = content_extractor._create_error_component("test_obj", "Test error message")
        
        assert error_comp.component_type == "extraction_error"
        assert error_comp.content == "Test error message"
        assert error_comp.extraction_method == "error_handler"
        assert "error" in error_comp.metadata

class TestAsyncBehavior(TestContentExtractor):
    """Test asynchronous operation correctness"""
    
    @pytest.mark.asyncio
    async def test_concurrent_extractions(self, content_extractor, test_files_dir):
        """Test multiple concurrent extractions"""
        
        test_files = [
            test_files_dir / "basic_types" / "sample.txt",
            test_files_dir / "basic_types" / "sample.json", 
            test_files_dir / "basic_types" / "sample.xml"
        ]
        
        # Run extractions concurrently
        tasks = []
        for i, file_path in enumerate(test_files):
            task = content_extractor.extract_from_file(
                str(file_path), f"concurrent_test_{i}", 1, i, "test_ds"
            )
            tasks.append(self._collect_components(task))
        
        results = await asyncio.gather(*tasks)
        
        # Verify all extractions completed
        assert len(results) == 3
        for result in results:
            assert len(result) >= 1  # At least one component per file
    
    async def _collect_components(self, component_iterator):
        """Helper to collect components from async iterator"""
        components = []
        async for component in component_iterator:
            components.append(component)
        return components
    
    @pytest.mark.asyncio
    async def test_temp_file_cleanup(self, content_extractor):
        """Test temporary file cleanup after processing"""
        
        # Process an archive to generate temp files
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            with zipfile.ZipFile(temp_zip_path, 'w') as zf:
                zf.writestr("test.txt", "Test content")
            
            initial_temp_count = len(content_extractor.temp_files_created)
            
            components = []
            async for component in content_extractor.extract_from_file(
                temp_zip_path, "test_cleanup", 1, 1, "test_ds"
            ):
                components.append(component)
            
            # Temp files should be cleaned up
            final_temp_count = len(content_extractor.temp_files_created)
            assert final_temp_count == initial_temp_count  # Should be back to initial state
            
        finally:
            os.unlink(temp_zip_path)

# Integration test for realistic scenarios
class TestIntegration(TestContentExtractor):
    """Integration tests for realistic extraction scenarios"""
    
    @pytest.mark.asyncio
    async def test_mixed_archive_processing(self, content_extractor, test_files_dir):
        """Test archive containing various file types"""
        
        # Create mixed content archive
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_zip:
            temp_zip_path = temp_zip.name
        
        try:
            with zipfile.ZipFile(temp_zip_path, 'w') as zf:
                # Add different file types
                zf.writestr("data.json", json.dumps({"user": "test", "ssn": "123-45-6789"}))
                zf.writestr("report.html", "<html><body><h1>Report</h1><p>SSN: 987-65-4321</p></body></html>")
                zf.writestr("summary.txt", "Summary with credit card: 4532-1234-5678-9012")
                zf.writestr("config.xml", "<?xml version='1.0'?><root><data>sensitive</data></root>")
            
            components = []
            async for component in content_extractor.extract_from_file(
                temp_zip_path, "test_mixed", 1, 1, "test_ds"
            ):
                components.append(component)
            
            # Verify all file types were processed
            extraction_methods = {c.extraction_method for c in components}
            expected_methods = {"json_parsing", "beautifulsoup_text", "text_file_read", "xml_parsing"}
            
            # Should have processed multiple file types
            assert len(extraction_methods.intersection(expected_methods)) >= 3
            
        finally:
            os.unlink(temp_zip_path)

# Test Runner Configuration
class TestRunner:
    """Test execution and reporting utilities"""
    
    @staticmethod
    def run_all_tests():
        """Run all test suites"""
        
        print("Content Extractor Test Suite")
        print("=" * 50)
        
        # Run tests with verbose output
        pytest_args = [
            "-v",  # Verbose
            "-s",  # Don't capture output
            "--tb=short",  # Short traceback format
            "--asyncio-mode=auto",  # Handle async tests
            __file__
        ]
        
        return pytest.main(pytest_args)
    
    @staticmethod
    def run_specific_test_class(test_class_name):
        """Run specific test class"""
        pytest_args = [
            "-v",
            "-s",
            f"{__file__}::{test_class_name}"
        ]
        return pytest.main(pytest_args)

if __name__ == "__main__":
    # Run all tests
    TestRunner.run_all_tests()