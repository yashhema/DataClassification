

from enum import Enum

class ProcessingStrategy(Enum):
    """Processing strategy selection"""
    ROW_BASED_NO_NAMES = "row_based_no_names"          # Strategy #1
    ROW_BASED_WITH_NAMES = "row_based_with_names"      # Strategy #2
    MULTI_ROW_WITH_NAMES = "multi_row_with_names"      # Strategy #3
    MULTI_ROW_NO_NAMES = "multi_row_no_names"          # Strategy #4
    COLUMNAR = "columnar" 
