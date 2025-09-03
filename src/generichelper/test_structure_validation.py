"""
Simple test file to validate pytest setup and test file discovery
Run this first to ensure pytest is working correctly
"""

import pytest
import os
from pathlib import Path


class TestPytestSetup:
    """Basic tests to validate pytest configuration"""
    
    def test_pytest_working(self):
        """Basic test to verify pytest is functioning"""
        assert True
        print("Pytest is working!")
    
    def test_test_files_dir_fixture(self, test_files_dir):
        """Test that our custom fixture works"""
        print(f"Test files directory: {test_files_dir}")
        assert isinstance(test_files_dir, Path)
        
        # Check if directory exists
        if test_files_dir.exists():
            print(f"✓ Directory exists: {test_files_dir}")
            
            # List contents
            subdirs = [d.name for d in test_files_dir.iterdir() if d.is_dir()]
            print(f"Subdirectories found: {subdirs}")
        else:
            print(f"✗ Directory not found: {test_files_dir}")
            pytest.skip(f"Test directory not found: {test_files_dir}")
    
    def test_file_discovery(self, test_files_dir):
        """Test file discovery in test directories"""
        if not test_files_dir.exists():
            pytest.skip("Test directory not found")
        
        # Check for expected subdirectories
        expected_dirs = ["basic_types", "complex_content", "archives", "edge_cases", "security_tests"]
        
        for expected_dir in expected_dirs:
            dir_path = test_files_dir / expected_dir
            if dir_path.exists():
                files = list(dir_path.glob("*"))
                print(f"✓ {expected_dir}: {len(files)} files")
            else:
                print(f"✗ Missing directory: {expected_dir}")
    
    def test_basic_file_access(self, test_files_dir):
        """Test that we can read test files"""
        if not test_files_dir.exists():
            pytest.skip("Test directory not found")
        
        # Try to find and read a simple text file
        basic_types_dir = test_files_dir / "basic_types"
        if basic_types_dir.exists():
            txt_files = list(basic_types_dir.glob("*.txt"))
            if txt_files:
                test_file = txt_files[0]
                try:
                    with open(test_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    print(f"✓ Successfully read {test_file.name}: {len(content)} characters")
                    assert len(content) > 0
                except Exception as e:
                    print(f"✗ Failed to read {test_file}: {e}")
                    pytest.fail(f"Cannot read test file: {e}")


if __name__ == "__main__":
    # Run this specific test file
    import sys
    pytest.main([__file__, "-v", "-s"] + sys.argv[1:])