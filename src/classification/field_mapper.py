# classification/field_mapper.py
"""
Field position mapping for structured data processing.
Maps Presidio findings back to source fields with position tracking.
"""

from typing import Dict, Tuple, List, Any, Optional
from dataclasses import dataclass
import re


@dataclass
class FieldPositionMap:
    """Maps field positions in combined text"""
    field_name: str
    value_start: int
    value_end: int
    full_start: int
    full_end: int


class FieldMapper:
    """Maps Presidio findings back to source fields following best practices"""
    
    def __init__(self):
        self.separator = " | "
        self.field_value_separator = ":"
    
    def build_row_text_with_mapping(self,
                                   row_data: Dict[str, Any],
                                   filtered_fields: List[str]) -> Tuple[str, Dict[str, FieldPositionMap]]:
        """
        Build combined text and track field positions
        
        Args:
            row_data: Dictionary of column_name -> value
            filtered_fields: List of fields to include in text
            
        Returns:
            Tuple of (combined_text, position_map)
        """
        text_parts = []
        position_map = {}
        current_pos = 0
        
        for field_name in filtered_fields:
            if field_name not in row_data:
                continue
            
            field_value = str(row_data[field_name]) if row_data[field_name] is not None else ""
            
            # Skip empty values
            if not field_value.strip():
                continue
            
            # Build field text: "field_name:value"
            field_text = f"{field_name}{self.field_value_separator}{field_value}"
            
            # Calculate positions
            full_start = current_pos
            value_start = current_pos + len(field_name) + len(self.field_value_separator)
            value_end = value_start + len(field_value)
            full_end = current_pos + len(field_text)
            
            # Store position mapping
            position_map[field_name] = FieldPositionMap(
                field_name=field_name,
                value_start=value_start,
                value_end=value_end,
                full_start=full_start,
                full_end=full_end
            )
            
            text_parts.append(field_text)
            current_pos = full_end + len(self.separator)
        
        combined_text = self.separator.join(text_parts)
        return combined_text, position_map
    
    def extract_source_field(self,
                             finding_start: int,
                             finding_end: int,
                             position_map: Dict[str, FieldPositionMap]) -> Optional[str]:
        """
        Map Presidio finding position back to source field
        
        Args:
            finding_start: Start position of finding in combined text
            finding_end: End position of finding in combined text
            position_map: Field position mapping
            
        Returns:
            Source field name or None if not found
        """
        for field_name, pos_map in position_map.items():
            # Check if finding is within this field's value area
            if (finding_start >= pos_map.value_start and
                finding_end <= pos_map.value_end):
                return field_name
            
            # Check for overlap (partial matches)
            if (finding_start < pos_map.value_end and
                finding_end > pos_map.value_start):
                return field_name
        
        return None
    
    def validate_field_mapping(self,
                              findings: List[Any],
                              position_map: Dict[str, FieldPositionMap]) -> Dict[str, List[Any]]:
        """
        Validate and group findings by source field
        
        Args:
            findings: List of Presidio findings
            position_map: Field position mapping
            
        Returns:
            Dictionary mapping field_name -> list of findings
        """
        field_findings = {}
        unmapped_findings = []
        
        for finding in findings:
            source_field = self.extract_source_field(
                finding.start_position,
                finding.end_position,
                position_map
            )
            
            if source_field:
                if source_field not in field_findings:
                    field_findings[source_field] = []
                field_findings[source_field].append(finding)
            else:
                unmapped_findings.append(finding)
        
        # Log unmapped findings for debugging
        if unmapped_findings:
            print(f"Warning: {len(unmapped_findings)} findings could not be mapped to fields")
        
        return field_findings


def filter_fields_for_classification(table_metadata: Dict[str, Any]) -> List[str]:
    """
    Filter table columns to include only those likely to contain PII
    
    Args:
        table_metadata: Table schema information
        
    Returns:
        List of field names to include in classification
    """
    included_fields = []
    
    for column_name, column_info in table_metadata.get('columns', {}).items():
        data_type = column_info.get('data_type', '').upper()
        
        # Step 1: Data type filter
        if data_type in ['DATE', 'TIMESTAMP', 'BOOLEAN', 'BINARY']:
            continue  # Skip these types
        
        # Step 2: Contextual filter for numeric types
        if data_type in ['INT', 'BIGINT', 'NUMERIC', 'DECIMAL']:
            # Check column name for PII patterns
            column_lower = column_name.lower()
            
            # Include if matches PII patterns
            pii_patterns = [
                'account', 'passport', 'license', 'ssn', 'social',
                'credit_card', 'card', 'number', 'id_number'
            ]
            
            # Exclude if matches generic patterns
            generic_patterns = [
                'customer_id', 'order_id', 'item_id', 'user_id',
                'quantity', 'age', 'amount', 'price', 'count'
            ]
            
            if any(pattern in column_lower for pattern in generic_patterns):
                continue
            
            if any(pattern in column_lower for pattern in pii_patterns):
                included_fields.append(column_name)
        else:
            # Include text-based fields
            included_fields.append(column_name)
    
    return included_fields


# Test functions
def test_field_mapper():
    """Test field mapping functionality"""
    mapper = FieldMapper()
    
    # Test data
    row_data = {
        "customer_id": 1001,
        "customer_name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "555-123-4567",
        "empty_field": None
    }
    
    filtered_fields = ["customer_name", "email", "phone"]
    
    # Test text building and mapping
    combined_text, position_map = mapper.build_row_text_with_mapping(row_data, filtered_fields)
    
    print(f"Combined text: {combined_text}")
    print(f"Position map: {position_map}")
    
    # Test field extraction
    # Simulate finding at email position
    email_pos = position_map["email"]
    found_field = mapper.extract_source_field(
        email_pos.value_start,
        email_pos.value_end,
        position_map
    )
    
    assert found_field == "email", f"Expected 'email', got '{found_field}'"
    print("✅ Field mapping test passed!")


if __name__ == "__main__":
    test_field_mapper()