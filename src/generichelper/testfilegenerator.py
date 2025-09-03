#!/usr/bin/env python3
"""
Test File Generator for Content Extractor Testing

This script creates various test files for validating the Content Extractor.
Some files are generated programmatically, others provide guidance for manual creation.
"""

import os
import json
import zipfile
import tarfile
import tempfile
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET

class TestFileGenerator:
    def __init__(self, output_dir="test_files"):
        self.output_dir = Path(output_dir)
        self.create_directory_structure()
    
    def create_directory_structure(self):
        """Create the test directory structure"""
        directories = [
            "basic_types",
            "complex_content", 
            "archives",
            "edge_cases",
            "security_tests"
        ]
        
        for dir_name in directories:
            (self.output_dir / dir_name).mkdir(parents=True, exist_ok=True)
        
        print(f"Created directory structure in {self.output_dir}")
    
    def generate_all_files(self):
        """Generate all test files"""
        print("Generating test files...")
        
        # Basic types
        self.create_text_files()
        self.create_json_files()
        self.create_xml_files()
        self.create_html_files()
        self.create_email_files()
        
        # Complex content
        self.create_complex_html()
        self.create_complex_xml()
        
        # Archives
        self.create_test_archives()
        
        # Edge cases
        self.create_edge_case_files()
        
        # Security test files
        self.create_security_test_files()
        
        # Instructions for manual file creation
        self.print_manual_creation_instructions()
    
    def create_text_files(self):
        """Create various text files"""
        
        # Simple text file
        simple_text = """Sample Text Document
        
This is a test document for text extraction validation.
It contains multiple lines and paragraphs.

Key testing points:
- Multiple paragraphs
- Special characters: àáâãäå, 中文, العربية
- Numbers: 123-456-7890
- Emails: test@example.com
- URLs: https://example.com
- Credit card format: 4532-1234-5678-9012

This text should be extracted as a single text component.
"""
        
        # Large text file (for size limit testing)
        large_text = "Large Text Content\n" + "Lorem ipsum dolor sit amet. " * 1000
        
        # UTF-8 with special characters
        unicode_text = """Unicode Test Document
        
Various Unicode characters:
- Emoji: 🚀 📊 💡 ⚠️
- Math symbols: ∑ ∞ π ∆ ∇
- Currency: $ € £ ¥ ₹
- Arrows: → ← ↑ ↓ ⟷
- Greek: α β γ δ ε θ λ μ π σ φ ψ ω
- Cyrillic: А Б В Г Д Е Ж З И Й К Л М Н О П Р С Т У Ф Х Ц Ч Ш Щ Ы Э Ю Я
"""
        
        self._write_file("basic_types/sample.txt", simple_text)
        self._write_file("edge_cases/large_text.txt", large_text)
        self._write_file("basic_types/unicode.txt", unicode_text)
    
    def create_json_files(self):
        """Create JSON test files"""
        
        # Simple JSON
        simple_data = {
            "name": "John Doe",
            "email": "john.doe@example.com",
            "ssn": "123-45-6789",
            "credit_card": "4532-1234-5678-9012",
            "phone": "555-123-4567",
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "zip": "12345"
            }
        }
        
        # Complex nested JSON
        complex_data = {
            "users": [
                {
                    "id": 1,
                    "personal_info": {
                        "name": "Alice Smith",
                        "ssn": "987-65-4321",
                        "dob": "1985-03-15",
                        "medical_record": "MRN-789456"
                    },
                    "financial": {
                        "accounts": [
                            {"type": "checking", "number": "12345678901", "balance": 5000},
                            {"type": "savings", "number": "10987654321", "balance": 15000}
                        ]
                    }
                },
                {
                    "id": 2,
                    "personal_info": {
                        "name": "Bob Johnson",
                        "ssn": "555-44-3333",
                        "dob": "1979-11-22",
                        "medical_record": "MRN-456123"
                    }
                }
            ],
            "metadata": {
                "export_date": "2025-01-15",
                "total_records": 2,
                "classification": "CONFIDENTIAL"
            }
        }
        
        # Array of objects (table-like)
        tabular_data = [
            {"employee_id": "E001", "name": "Sarah Wilson", "ssn": "111-22-3333", "salary": 75000},
            {"employee_id": "E002", "name": "Mike Davis", "ssn": "444-55-6666", "salary": 68000},
            {"employee_id": "E003", "name": "Lisa Chen", "ssn": "777-88-9999", "salary": 82000}
        ]
        
        self._write_json("basic_types/sample.json", simple_data)
        self._write_json("complex_content/complex_nested.json", complex_data)
        self._write_json("basic_types/tabular_data.json", tabular_data)
    
    def create_xml_files(self):
        """Create XML test files"""
        
        # Simple XML
        root = ET.Element("document")
        ET.SubElement(root, "title").text = "Sample XML Document"
        ET.SubElement(root, "content").text = "This XML contains sensitive data like SSN: 123-45-6789"
        ET.SubElement(root, "author").text = "Test Author"
        ET.SubElement(root, "classification").text = "CONFIDENTIAL"
        
        # XML with tabular data (repeating elements)
        employees_root = ET.Element("employees")
        
        employees = [
            {"id": "1", "name": "John Smith", "ssn": "123-45-6789", "department": "Engineering"},
            {"id": "2", "name": "Jane Doe", "ssn": "987-65-4321", "department": "Marketing"},
            {"id": "3", "name": "Bob Wilson", "ssn": "555-44-3333", "department": "Finance"}
        ]
        
        for emp in employees:
            emp_elem = ET.SubElement(employees_root, "employee")
            for key, value in emp.items():
                ET.SubElement(emp_elem, key).text = value
        
        self._write_xml("basic_types/sample.xml", root)
        self._write_xml("complex_content/employees.xml", employees_root)
    
    def create_html_files(self):
        """Create HTML test files"""
        
        # Simple HTML
        simple_html = """<!DOCTYPE html>
<html>
<head>
    <title>Test HTML Document</title>
</head>
<body>
    <h1>Sample HTML Content</h1>
    <p>This document contains sensitive information:</p>
    <ul>
        <li>Social Security Number: 123-45-6789</li>
        <li>Credit Card: 4532-1234-5678-9012</li>
        <li>Phone: (555) 123-4567</li>
    </ul>
    
    <h2>Contact Information</h2>
    <p>For more information, contact john.doe@example.com</p>
    
    <!-- Reference to image that should be processed -->
    <img src="sample_chart.png" alt="Sales Chart">
</body>
</html>"""
        
        # HTML with tables
        html_with_tables = """<!DOCTYPE html>
<html>
<head>
    <title>Employee Data</title>
</head>
<body>
    <h1>Employee Information</h1>
    
    <table border="1">
        <tr>
            <th>Employee ID</th>
            <th>Name</th>
            <th>SSN</th>
            <th>Department</th>
            <th>Salary</th>
        </tr>
        <tr>
            <td>E001</td>
            <td>Alice Johnson</td>
            <td>123-45-6789</td>
            <td>Engineering</td>
            <td>$75,000</td>
        </tr>
        <tr>
            <td>E002</td>
            <td>Bob Smith</td>
            <td>987-65-4321</td>
            <td>Marketing</td>
            <td>$68,000</td>
        </tr>
        <tr>
            <td>E003</td>
            <td>Carol Wilson</td>
            <td>555-44-3333</td>
            <td>Finance</td>
            <td>$82,000</td>
        </tr>
    </table>
    
    <h2>Financial Summary</h2>
    <table border="1">
        <tr>
            <th>Account Type</th>
            <th>Account Number</th>
            <th>Balance</th>
        </tr>
        <tr>
            <td>Checking</td>
            <td>1234-5678-9012</td>
            <td>$5,000.00</td>
        </tr>
        <tr>
            <td>Savings</td>
            <td>9876-5432-1098</td>
            <td>$25,000.00</td>
        </tr>
    </table>
</body>
</html>"""
        
        self._write_file("basic_types/sample.html", simple_html)
        self._write_file("complex_content/tables.html", html_with_tables)
    
    def create_email_files(self):
        """Create EML test files"""
        
        # Simple email
        simple_email = """From: sender@example.com
To: recipient@example.com  
Subject: Confidential Employee Information
Date: Mon, 15 Jan 2025 10:30:00 -0500

Hello,

Please find the employee details below:

Name: John Doe
SSN: 123-45-6789
Employee ID: E12345
Salary: $75,000

This information is confidential.

Best regards,
HR Department
"""

        # HTML email with table
        html_email = """From: hr@company.com
To: manager@company.com
Subject: Q1 Employee Report
Date: Tue, 16 Jan 2025 14:20:00 -0500
Content-Type: multipart/alternative; boundary="boundary123"

--boundary123
Content-Type: text/plain

Q1 Employee Report - Plain Text Version

Employee Data:
- John Smith (123-45-6789) - $75,000
- Jane Doe (987-65-4321) - $68,000

--boundary123
Content-Type: text/html

<html>
<body>
<h2>Q1 Employee Report</h2>
<table border="1">
<tr><th>Name</th><th>SSN</th><th>Salary</th></tr>
<tr><td>John Smith</td><td>123-45-6789</td><td>$75,000</td></tr>
<tr><td>Jane Doe</td><td>987-65-4321</td><td>$68,000</td></tr>
</table>
</body>
</html>

--boundary123--
"""
        
        self._write_file("basic_types/sample.eml", simple_email)
        self._write_file("complex_content/html_email.eml", html_email)
    
    def create_test_archives(self):
        """Create test archive files"""
        
        # Simple ZIP archive
        simple_zip_path = self.output_dir / "archives" / "simple.zip"
        with zipfile.ZipFile(simple_zip_path, 'w') as zf:
            # Add text file
            zf.writestr("readme.txt", "This is a text file inside a ZIP archive.\nSSN: 123-45-6789")
            
            # Add JSON file
            zf.writestr("data.json", json.dumps({
                "user": "test_user",
                "ssn": "987-65-4321", 
                "account": "ACC-123456"
            }, indent=2))
            
            # Add HTML file
            zf.writestr("report.html", """<html><body>
            <h1>Archive Report</h1>
            <p>Credit Card: 4532-1234-5678-9012</p>
            </body></html>""")
        
        # Nested ZIP archive
        nested_zip_path = self.output_dir / "archives" / "nested.zip"
        with zipfile.ZipFile(nested_zip_path, 'w') as outer_zip:
            # Create inner ZIP in memory using BytesIO
            from io import BytesIO
            inner_zip_buffer = BytesIO()
            
            with zipfile.ZipFile(inner_zip_buffer, 'w') as inner_zip:
                inner_zip.writestr("nested_data.txt", "Nested file content with SSN: 111-22-3333")
                inner_zip.writestr("deep_data.json", json.dumps({
                    "patient": "Jane Smith",
                    "mrn": "MRN-999888777",
                    "diagnosis": "confidential"
                }))
            
            inner_zip_data = inner_zip_buffer.getvalue()
            outer_zip.writestr("inner_archive.zip", inner_zip_data)
            outer_zip.writestr("outer_file.txt", "Outer archive file with phone: 555-987-6543")
        
        # TAR archive
        tar_path = self.output_dir / "archives" / "sample.tar.gz"
        with tarfile.open(tar_path, 'w:gz') as tf:
            # Create temp files to add
            temp_dir = tempfile.mkdtemp()
            
            # Text file
            text_file = os.path.join(temp_dir, "archive_text.txt")
            with open(text_file, 'w') as f:
                f.write("TAR archive content with medical record: MRN-789456")
            tf.add(text_file, "archive_text.txt")
            
            # JSON file
            json_file = os.path.join(temp_dir, "archive_data.json")
            with open(json_file, 'w') as f:
                json.dump({"patient_id": "P123", "diagnosis": "confidential"}, f)
            tf.add(json_file, "archive_data.json")
        
        print(f"Created archives in {self.output_dir / 'archives'}")
    
    def create_edge_case_files(self):
        """Create edge case test files"""
        
        # Empty files
        self._write_file("edge_cases/empty.txt", "")
        self._write_file("edge_cases/empty.json", "{}")
        self._write_file("edge_cases/empty.xml", "<?xml version='1.0'?><root></root>")
        
        # Large file (approaching size limits)
        large_content = "Large file content. " * 50000  # ~1MB of text
        self._write_file("edge_cases/large_file.txt", large_content)
        
        # File with wrong extension (JSON content in .txt file)
        wrong_ext_content = json.dumps({"data": "This JSON is in a .txt file", "ssn": "123-45-6789"})
        self._write_file("edge_cases/json_as_txt.txt", wrong_ext_content)
        
        # Malformed JSON
        self._write_file("edge_cases/malformed.json", '{"invalid": json, "missing": }')
        
        # Malformed XML
        self._write_file("edge_cases/malformed.xml", "<root><unclosed><data>content</root>")
    
    def create_security_test_files(self):
        """Create security test archives"""
        
        # Path traversal ZIP
        malicious_zip_path = self.output_dir / "security_tests" / "path_traversal.zip"
        with zipfile.ZipFile(malicious_zip_path, 'w') as zf:
            # Attempt various path traversal attacks
            zf.writestr("../../../etc/passwd", "root:x:0:0:root:/root:/bin/bash")
            zf.writestr("..\\..\\windows\\system32\\config\\sam", "Windows registry content")
            zf.writestr("normal_file.txt", "This is a legitimate file")
            zf.writestr("/absolute/path/file.txt", "Absolute path attempt")
        
        # ZIP with many files (member count limit test)
        many_files_zip = self.output_dir / "security_tests" / "many_files.zip"
        with zipfile.ZipFile(many_files_zip, 'w') as zf:
            for i in range(150):  # Exceed typical limits
                zf.writestr(f"file_{i:03d}.txt", f"Content of file {i} with data: {i*123}")
        
        # Create circular reference archive manually
        # (This needs to be done carefully to avoid infinite loops)
        
        print(f"Created security test files in {self.output_dir / 'security_tests'}")
    
    def create_complex_html(self):
        """Create complex HTML with multiple features"""
        
        complex_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Complex Test Document</title>
    <style>
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        .sensitive { background-color: #ffe6e6; }
    </style>
</head>
<body>
    <h1>Employee Database Export</h1>
    
    <p>This document contains confidential employee information exported on 
    <strong>January 15, 2025</strong> for internal use only.</p>
    
    <h2>Employee Records</h2>
    <table>
        <thead>
            <tr>
                <th>Employee ID</th>
                <th>Full Name</th>
                <th>Social Security Number</th>
                <th>Email Address</th>
                <th>Phone Number</th>
                <th>Department</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>EMP-001</td>
                <td>Michael Johnson</td>
                <td class="sensitive">123-45-6789</td>
                <td>m.johnson@company.com</td>
                <td>(555) 123-4567</td>
                <td>Engineering</td>
            </tr>
            <tr>
                <td>EMP-002</td>
                <td>Sarah Williams</td>
                <td class="sensitive">987-65-4321</td>
                <td>s.williams@company.com</td>
                <td>(555) 234-5678</td>
                <td>Marketing</td>
            </tr>
            <tr>
                <td>EMP-003</td>
                <td>David Brown</td>
                <td class="sensitive">555-44-3333</td>
                <td>d.brown@company.com</td>
                <td>(555) 345-6789</td>
                <td>Finance</td>
            </tr>
        </tbody>
    </table>
    
    <h2>Financial Information</h2>
    <table>
        <thead>
            <tr>
                <th>Account Type</th>
                <th>Account Number</th>
                <th>Routing Number</th>
                <th>Balance</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Business Checking</td>
                <td>1234567890123456</td>
                <td>021000021</td>
                <td>$125,000.00</td>
            </tr>
            <tr>
                <td>Payroll Account</td>
                <td>6543210987654321</td>
                <td>021000021</td>
                <td>$45,000.00</td>
            </tr>
        </tbody>
    </table>
    
    <h2>Medical Records Summary</h2>
    <p>The following employees have submitted medical documentation:</p>
    <ul>
        <li>Michael Johnson - Medical Record Number: MRN-789456123</li>
        <li>Sarah Williams - Medical Record Number: MRN-654321789</li>
    </ul>
    
    <div class="footer">
        <p><em>This document is classified as CONFIDENTIAL and should not be distributed.</em></p>
        <!-- Image reference for OCR testing -->
        <img src="confidential_chart.jpg" alt="Confidential Performance Chart" width="400">
    </div>
</body>
</html>"""
        
        self._write_file("complex_content/complex_tables.html", complex_html)
    
    def create_complex_xml(self):
        """Create complex XML with nested structures"""
        
        complex_xml = """<?xml version="1.0" encoding="UTF-8"?>
<hospital_records>
    <metadata>
        <export_date>2025-01-15T10:30:00Z</export_date>
        <classification>PHI_CONFIDENTIAL</classification>
        <total_patients>3</total_patients>
    </metadata>
    
    <patients>
        <patient id="P001">
            <personal_info>
                <name>John Adams</name>
                <ssn>123-45-6789</ssn>
                <dob>1985-03-15</dob>
                <mrn>MRN-789456123</mrn>
            </personal_info>
            <contact>
                <phone>(555) 123-4567</phone>
                <email>j.adams@email.com</email>
                <address>
                    <street>123 Main Street</street>
                    <city>Springfield</city>
                    <zip>12345</zip>
                </address>
            </contact>
            <medical>
                <diagnosis>Hypertension</diagnosis>
                <physician>Dr. Smith</physician>
                <visit_date>2025-01-10</visit_date>
            </medical>
        </patient>
        
        <patient id="P002">
            <personal_info>
                <name>Mary Johnson</name>
                <ssn>987-65-4321</ssn>
                <dob>1978-11-22</dob>
                <mrn>MRN-654321789</mrn>
            </personal_info>
            <contact>
                <phone>(555) 234-5678</phone>
                <email>m.johnson@email.com</email>
            </contact>
            <medical>
                <diagnosis>Diabetes Type 2</diagnosis>
                <physician>Dr. Brown</physician>
                <visit_date>2025-01-12</visit_date>
            </medical>
        </patient>
        
        <patient id="P003">
            <personal_info>
                <name>Robert Wilson</name>
                <ssn>555-44-3333</ssn>
                <dob>1990-07-08</dob>
                <mrn>MRN-333444555</mrn>
            </personal_info>
            <contact>
                <phone>(555) 345-6789</phone>
                <email>r.wilson@email.com</email>
            </contact>
        </patient>
    </patients>
</hospital_records>"""
        
        self._write_file("complex_content/hospital_records.xml", complex_xml)
    
    def print_manual_creation_instructions(self):
        """Print instructions for manually creating binary files"""
        
        instructions = """
MANUAL FILE CREATION INSTRUCTIONS
=================================

The following files need to be created manually or downloaded:

BASIC TYPES:
-----------
1. sample.pdf - Create a PDF with:
   - Text content including SSN: 123-45-6789
   - At least one table with employee data
   - An embedded image with text (for OCR testing)

2. sample.docx - Create a Word document with:
   - Text paragraphs with sensitive data
   - A table with employee information
   - Embedded images containing text

3. sample.xlsx - Create an Excel file with:
   - Multiple sheets (Sheet1: employees, Sheet2: finances)
   - Formulas and formatting
   - Embedded charts/images
   - Data like SSNs, account numbers

4. sample.pptx - Create a PowerPoint with:
   - Title slide with company info
   - Slide with table of employee data  
   - Slide with images containing text
   - Speaker notes with sensitive information

5. Images for OCR testing:
   - sample_chart.png - Chart with text labels
   - confidential_chart.jpg - Image with sensitive data
   - clear_text.png - High-quality text image
   - poor_quality.jpg - Low-quality/blurry text

EDGE CASES:
----------
6. corrupted.pdf - Intentionally corrupted PDF file
7. password_protected.docx - Password-protected Word document
8. oversized.xlsx - Excel file larger than configured limits

SECURITY TESTS:
--------------
9. zip_bomb.zip - ZIP archive that expands to huge size
10. circular_reference.zip - ZIP containing reference to itself

COMPLEX ARCHIVES:
----------------
11. office_archive.zip - ZIP containing mix of Office documents
12. deep_nested.zip - ZIP with multiple levels of nesting
13. mixed_content.tar.gz - TAR with various file types

DOWNLOAD SOURCES:
----------------
You can find sample files at:
- PDF samples: Adobe's sample PDFs
- Office samples: Microsoft's template gallery
- Test images with text: Generate using online tools
- Archive samples: Create using the provided Python code

AUTOMATION SCRIPT:
-----------------
Run this generator script to create all the text-based files:

python test_file_generator.py

Then manually add the binary files to complete the test suite.
"""
        
        print(instructions)
        
        # Save instructions to file
        self._write_file("FILE_CREATION_INSTRUCTIONS.txt", instructions)
    
    def _write_file(self, relative_path, content):
        """Helper to write file with directory creation"""
        file_path = self.output_dir / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
    
    def _write_json(self, relative_path, data):
        """Helper to write JSON file"""
        file_path = self.output_dir / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _write_xml(self, relative_path, root_element):
        """Helper to write XML file"""
        file_path = self.output_dir / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        tree = ET.ElementTree(root_element)
        ET.indent(tree, space="  ", level=0)
        tree.write(file_path, encoding='utf-8', xml_declaration=True)

if __name__ == "__main__":
    generator = TestFileGenerator("test_files")
    generator.generate_all_files()
    
    print("\n" + "="*50)
    print("TEST FILE GENERATION COMPLETE")
    print("="*50)
    print(f"Files created in: {generator.output_dir}")
    print("\nNext steps:")
    print("1. Run this script to generate text-based files")
    print("2. Manually create binary files (PDF, Office, images) as instructed")
    print("3. Use the generated files with your Content Extractor tests")
    print("\nAll files contain realistic sensitive data patterns for testing classification.")