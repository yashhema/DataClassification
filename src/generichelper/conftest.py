"""
conftest.py - pytest configuration and fixtures

This file must be in the same directory as your test files for pytest
to recognize the custom command line options.
"""

import pytest
from pathlib import Path

def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--test-files-dir",
        action="store", 
        default="test_files",
        help="Directory containing test files for extraction testing"
    )

@pytest.fixture
def test_files_dir(request):
    """Get test files directory from command line option"""
    test_dir = request.config.getoption("--test-files-dir")
    test_path = Path(test_dir)
    
    # Verify test directory exists
    if not test_path.exists():
        pytest.skip(f"Test files directory not found: {test_path}")
    
    return test_path