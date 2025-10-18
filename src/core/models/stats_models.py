from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.models.models import PIIFinding

@dataclass
class ColumnStatistics:
    """Column-level statistics (calculated once per column)"""
    field_name: str
    total_rows_scanned: int
    
    # Null statistics
    null_count: int
    null_percentage: float
    
    # Length statistics
    min_value_length: int
    max_value_length: int
    mean_value_length: float
    
    # Uniqueness statistics
    distinct_value_count: int
    distinct_value_percentage: float
    
    # Common values
    most_common_value: Optional[str]
    most_common_value_count: int
    most_common_value_percentage: float
    
    # Data quality
    has_truncated_values: bool = False
    truncated_value_count: int = 0

@dataclass
class MatchStatistics:
    """Match statistics per classifier (updated incrementally)"""
    classifier_id: str
    entity_type: str
    field_name: str
    
    # Regex pattern matches
    total_regex_matches: int = 0
    regex_match_rate: float = 0.0
    distinct_matches: set = None  # Will become int after finalize()
    distinct_match_percentage: float = 0.0  # ✅ ADD THIS
    
    # Dictionary-based matches
    column_name_matched: bool = False
    words_match_count: int = 0
    words_match_rate: float = 0.0  # ✅ ADD THIS (if calculated)
    exact_match_count: int = 0
    exact_match_rate: float = 0.0  # ✅ ADD THIS (if calculated)
    negative_match_count: int = 0
    negative_match_rate: float = 0.0  # ✅ ADD THIS (if calculated)
    
    # Cross-column boosts
    cross_column_boost_count: int = 0
    cross_column_boost_total: float = 0.0
    
    def __post_init__(self):
        if self.distinct_matches is None:
            self.distinct_matches = set()
    
    def update_with_finding(self, finding: 'PIIFinding', applied_boosts: dict):
        """Update statistics with a new finding"""
        self.total_regex_matches += 1
        self.distinct_matches.add(finding.text)
        
        if applied_boosts.get('column_name_boost', 0) > 0:
            self.column_name_matched = True
        if applied_boosts.get('words_boost', 0) > 0:
            self.words_match_count += 1
        if applied_boosts.get('exact_match_boost', 0) > 0:
            self.exact_match_count += 1
        if applied_boosts.get('negative_penalty', 0) < 0:
            self.negative_match_count += 1
        if applied_boosts.get('cross_column_boost', 0) > 0:
            self.cross_column_boost_count += 1
            self.cross_column_boost_total += applied_boosts['cross_column_boost']
    
    def finalize(self, total_rows: int):
        """Calculate final percentages after all findings processed"""
        if total_rows > 0:
            self.regex_match_rate = (self.total_regex_matches / total_rows * 100)
            self.distinct_match_percentage = (len(self.distinct_matches) / total_rows * 100)
            self.words_match_rate = (self.words_match_count / total_rows * 100)
            self.exact_match_rate = (self.exact_match_count / total_rows * 100)
            self.negative_match_rate = (self.negative_match_count / total_rows * 100)
        else:
            self.regex_match_rate = 0.0
            self.distinct_match_percentage = 0.0
            self.words_match_rate = 0.0
            self.exact_match_rate = 0.0
            self.negative_match_rate = 0.0
        
        # Convert set to count for serialization
        distinct_count = len(self.distinct_matches)
        self.distinct_matches = distinct_count  # Replace set with count

@dataclass
class ConfidenceComponents:
    """Breakdown of confidence score calculation"""
    presidio_base_score: float
    column_name_boost: float = 0.0
    words_boost: float = 0.0
    exact_match_boost: float = 0.0
    negative_penalty: float = 0.0
    cross_column_boost: float = 0.0
    final_confidence: float = 0.0
    validation_boost: float = 0.0    
    def calculate_final(self) -> float:
        """
        Simple sum of all components (for backward compatibility).
        NOTE: Use _calculate_weighted_confidence() for production.
        """
        self.final_confidence = min(1.0, max(0.0,
            self.presidio_base_score +
            self.column_name_boost +
            self.words_boost +
            self.exact_match_boost +
            self.negative_penalty +
            self.cross_column_boost +
            self.validation_boost
        ))
        return self.final_confidence
