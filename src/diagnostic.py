#!/usr/bin/env python3
"""
Diagnostic script to check what imports are failing
Run this in your src directory: python diagnostic.py
"""

import sys
import traceback

def test_import(module_path, description):
    """Test if a module can be imported"""
    try:
        exec(f"import {module_path}")
        print(f"✅ {description}: {module_path}")
        return True
    except ImportError as e:
        print(f"❌ {description}: {module_path}")
        print(f"   Error: {e}")
        return False
    except Exception as e:
        print(f"⚠️  {description}: {module_path}")
        print(f"   Unexpected error: {e}")
        return False

def test_from_import(module_path, items, description):
    """Test if specific items can be imported from a module"""
    try:
        items_str = ", ".join(items)
        exec(f"from {module_path} import {items_str}")
        print(f"✅ {description}: from {module_path} import {items_str}")
        return True
    except ImportError as e:
        print(f"❌ {description}: from {module_path} import {items_str}")
        print(f"   Error: {e}")
        return False
    except Exception as e:
        print(f"⚠️  {description}: from {module_path} import {items_str}")
        print(f"   Unexpected error: {e}")
        return False

print("=== Content Extractor Import Diagnostics ===")
print(f"Python path: {sys.path}")
print(f"Current directory: {sys.path[0]}")
print()

# Test main imports that your test file needs
imports_to_test = [
    # Main module
    ("content_extraction.content_extractor", "ContentExtractor module"),
    
    # Core models
    ("core.models.models", "Core models"),
    ("core.logging.system_logger", "System logger"),
    ("core.config.configuration_manager", "Configuration manager"),
    ("core.errors", "Error handler"),
    
    # Python standard libraries
    ("pathlib", "Path library"),
    ("tempfile", "Temp file library"),
    ("zipfile", "ZIP file library"),
    ("tarfile", "TAR file library"),
    
    # Third-party libraries your code needs
    ("pytest", "PyTest"),
    ("aiofiles", "Async file operations"),
]

# Test from imports
from_imports_to_test = [
    ("content_extraction.content_extractor", ["ContentExtractor"], "ContentExtractor class"),
    ("core.models.models", ["ContentComponent", "ContentExtractionConfig"], "Model classes"),
    ("core.logging.system_logger", ["SystemLogger"], "Logger class"),
    ("core.config.configuration_manager", ["SystemConfig"], "Config class"),
    ("core.errors", ["ErrorHandler"], "Error handler class"),
]

success_count = 0
total_count = 0

print("--- Testing Module Imports ---")
for module_path, description in imports_to_test:
    total_count += 1
    if test_import(module_path, description):
        success_count += 1
    print()

print("--- Testing Specific Class Imports ---")
for module_path, items, description in from_imports_to_test:
    total_count += 1
    if test_from_import(module_path, items, description):
        success_count += 1
    print()

print("--- Testing Optional Dependencies ---")
optional_deps = [
    ("pymupdf", "PyMuPDF for PDF processing"),
    ("pandas", "Pandas for data processing"),
    ("openpyxl", "OpenPyXL for Excel files"),
    ("docx", "python-docx for Word documents"),
    ("pptx", "python-pptx for PowerPoint"),
    ("bs4", "BeautifulSoup for HTML"),
    ("easyocr", "EasyOCR for image text"),
    ("torch", "PyTorch for ML operations"),
    ("magic", "python-magic for file type detection"),
]

for module_path, description in optional_deps:
    test_import(module_path, description)
    print()

print(f"=== Summary ===")
print(f"Core imports successful: {success_count}/{total_count}")

if success_count < total_count:
    print("\n🚨 Issues found! This explains the VS Code errors.")
    print("Next steps:")
    print("1. Install missing dependencies: pip install [missing-package]")
    print("2. Fix import paths to match your actual project structure")
    print("3. Create missing model classes if they don't exist")
else:
    print("\n✅ All core imports working! VS Code errors might be due to:")
    print("1. VS Code Python interpreter not pointing to right environment")
    print("2. VS Code workspace settings")
    print("3. Missing __init__.py files in package directories")