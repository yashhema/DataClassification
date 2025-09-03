#!/usr/bin/env python3
"""
Import Validation Script
Validates that all Python modules in the project can be imported successfully
"""

import os
import sys
import importlib.util
from pathlib import Path

def validate_project_imports(src_directory):
    """Validate all Python files can be imported"""
    
    src_path = Path(src_directory)
    if not src_path.exists():
        print(f"Error: Directory not found: {src_path}")
        return False
    
    # Add src directory to Python path
    sys.path.insert(0, str(src_path))
    
    # Find all Python files
    python_files = []
    for root, dirs, files in os.walk(src_path):
        for file in files:
            if file.endswith('.py') and not file.startswith('__'):
                rel_path = Path(root).relative_to(src_path)
                if rel_path == Path('.'):
                    module_name = file[:-3]  # Remove .py
                else:
                    module_name = str(rel_path / file[:-3]).replace(os.sep, '.')
                python_files.append((Path(root) / file, module_name))
    
    print(f"Found {len(python_files)} Python files to validate")
    print("=" * 60)
    
    successful_imports = []
    failed_imports = []
    
    for file_path, module_name in python_files:
        try:
            # Try to load the module
            

            importlib.import_module(module_name)
            successful_imports.append(module_name)
            print(f"✓ {module_name}")
            
        except ImportError as e:
            failed_imports.append((module_name, f"ImportError: {e}"))
            print(f"✗ {module_name}: Import failed - {e}")
            
        except SyntaxError as e:
            failed_imports.append((module_name, f"SyntaxError: {e}"))
            print(f"✗ {module_name}: Syntax error - {e}")
            
        except Exception as e:
            failed_imports.append((module_name, f"Error: {e}"))
            print(f"✗ {module_name}: {type(e).__name__} - {e}")
    
    print("=" * 60)
    print(f"Results: {len(successful_imports)} successful, {len(failed_imports)} failed")
    
    if failed_imports:
        print("\nFailed imports:")
        for module, error in failed_imports:
            print(f"  - {module}: {error}")
        return False
    else:
        print("All modules imported successfully!")
        return True

def check_specific_modules():
    """Check specific modules needed for Content Extractor"""
    
    modules_to_check = [
        "core.models.models",
        "core.logging.system_logger", 
        "core.config.configuration_manager",
        "core.errors",
        "content_extraction.content_extractor"
    ]
    
    print("Checking specific Content Extractor dependencies:")
    print("-" * 50)
    
    for module_name in modules_to_check:
        try:
            module = importlib.import_module(module_name)
            print(f"✓ {module_name}")
            
            # Check for specific classes
            if module_name == "core.models":
                if hasattr(module, 'ContentComponent'):
                    print(f"  ✓ ContentComponent found")
                if hasattr(module, 'ContentExtractionConfig'):
                    print(f"  ✓ ContentExtractionConfig found")
                    
            elif module_name == "extraction.content_extractor_implementation":
                if hasattr(module, 'ContentExtractor'):
                    print(f"  ✓ ContentExtractor class found")
                else:
                    print(f"  ✗ ContentExtractor class not found")
                    # List available classes
                    classes = [name for name in dir(module) if not name.startswith('_')]
                    print(f"    Available: {classes}")
            
        except ImportError as e:
            print(f"✗ {module_name}: {e}")
        except Exception as e:
            print(f"✗ {module_name}: {type(e).__name__} - {e}")

def check_optional_dependencies():
    """Check optional libraries for Content Extractor"""
    
    optional_libs = {
        "pymupdf": "PDF processing",
        "camelot": "PDF table extraction", 
        "tabula": "PDF table extraction fallback",
        "docx": "Word document processing",
        "openpyxl": "Excel processing",
        "pptx": "PowerPoint processing",
        "bs4": "HTML processing",
        "easyocr": "OCR capabilities",
        "magic": "File type detection",
        "aiofiles": "Async file operations"
    }
    
    print("\nChecking optional dependencies:")
    print("-" * 40)
    
    for lib, description in optional_libs.items():
        try:
            importlib.import_module(lib)
            print(f"✓ {lib} - {description}")
        except ImportError:
            print(f"✗ {lib} - {description} (missing)")

if __name__ == "__main__":
    # Get src directory
    if len(sys.argv) > 1:
        src_dir = sys.argv[1]
    else:
        src_dir = r"C:\PAM_Solutiom\Db_Scan_Job\DataClassification\src"
    
    print(f"Validating Python project: {src_dir}")
    print("=" * 60)
    
    # Validate all imports
    success = validate_project_imports(src_dir)
    
    # Check specific modules
    print("\n")
    check_specific_modules()
    
    # Check optional dependencies
    check_optional_dependencies()
    
    if success:
        print("\n🎉 Project validation successful!")
        sys.exit(0)
    else:
        print("\n❌ Project validation failed - fix import errors before running tests")
        sys.exit(1)