# classification/field_mapper.py
"""
Field position mapping for structured data processing.
Maps Presidio findings back to source fields with position tracking.
"""

from typing import Dict, Tuple, List, Any, Optional
from dataclasses import dataclass
from .generic_enums import ProcessingStrategy
MAX_CHUNK_SIZE = 45000
MAX_VALUE_SIZE = 50000
MAX_ROWS_PER_COLUMN = 30000
@dataclass
class FieldPositionMap:
    """Maps finding positions to source location"""
    row_idx: int              # Source row index
    field_name: str           # Source column name
    value_start: int          # Value start position in chunk text
    value_end: int            # Value end position in chunk text
    full_start: int           # Full block start (includes field name if present)
    full_end: int             # Full block end
    chunk_id: str             # Chunk identifier for tracking


class FieldMapper:
    """Maps Presidio findings back to source fields following best practices"""
    
    def __init__(self):
        self.field_separator = " | "        # Rename from 'separator'
        self.field_value_separator = ":"
        self.row_separator = " || " 

    def build_row_text_with_mapping(
        self,
        row_data: Dict[str, Any],
        row_idx: int,
        filtered_fields: List[str]
    ) -> Tuple[str, List[FieldPositionMap]]:
        """
        Build text for single row with field names (Strategy #2)
        
        Format: "field_name:value | field_name:value"
        
        Args:
            row_data: Dictionary of column_name -> value
            row_idx: The row index from source table
            filtered_fields: List of fields to include in text
            
        Returns:
            Tuple of (combined_text, position_map)
        """
        text_parts = []
        position_map = []
        current_pos = 0
        chunk_id = f"row_{row_idx}"
        
        for field_name in filtered_fields:
            if field_name not in row_data:
                continue
            
            # Safe string conversion with error handling
            try:
                field_value = str(row_data[field_name]) if row_data[field_name] is not None else ""
            except Exception as e:
                print(f"Warning: Failed to convert field '{field_name}' value to string in row {row_idx}: {e}")
                continue
            
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
            position_map.append(FieldPositionMap(
                row_idx=row_idx,
                field_name=field_name,
                value_start=value_start,
                value_end=value_end,
                full_start=full_start,
                full_end=full_end,
                chunk_id=chunk_id
            ))
            
            text_parts.append(field_text)
            current_pos = full_end + len(self.field_separator)
        
        combined_text = self.field_separator.join(text_parts)
        return combined_text, position_map


    def build_row_text_no_names(
        self,
        row_data: Dict[str, Any],
        row_idx: int,
        filtered_fields: List[str]
    ) -> Tuple[str, List[FieldPositionMap]]:
        """
        Build text for single row WITHOUT field names (Strategy #1)
        
        Format: "value1 | value2 | value3"
        
        Args:
            row_data: Dictionary of column_name -> value
            row_idx: The row index from source table
            filtered_fields: List of fields to include in text
            
        Returns:
            Tuple of (combined_text, position_map)
        """
        text_parts = []
        position_map = []
        current_pos = 0
        chunk_id = f"row_{row_idx}"
        
        for field_name in filtered_fields:
            if field_name not in row_data:
                continue
            
            # Safe string conversion with error handling
            try:
                field_value = str(row_data[field_name]) if row_data[field_name] is not None else ""
            except Exception as e:
                print(f"Warning: Failed to convert field '{field_name}' value to string in row {row_idx}: {e}")
                continue
            
            # Skip empty values
            if not field_value.strip():
                continue
            
            # Calculate positions (no field name, just value)
            value_start = current_pos
            value_end = current_pos + len(field_value)
            full_start = value_start
            full_end = value_end
            
            # Store position mapping (still track field_name for mapping back)
            position_map.append(FieldPositionMap(
                row_idx=row_idx,
                field_name=field_name,
                value_start=value_start,
                value_end=value_end,
                full_start=full_start,
                full_end=full_end,
                chunk_id=chunk_id
            ))
            
            text_parts.append(field_value)
            current_pos = value_end + len(self.field_separator)
        
        combined_text = self.field_separator.join(text_parts)
        return combined_text, position_map


    def build_multirow_text_with_names(
        self,
        rows_data: List[Dict[str, Any]],
        start_row_idx: int,
        filtered_fields: List[str],
        max_chunk_size: int = MAX_CHUNK_SIZE
    ) -> List[Tuple[str, List[FieldPositionMap], str]]:
        """
        Build text for MULTIPLE rows with field names, chunked (Strategy #3)
        
        Format: "name:val | email:val || name:val | email:val"
        Row separator: " || "
        
        Args:
            rows_data: List of row dictionaries
            start_row_idx: Starting row index from source table
            filtered_fields: List of fields to include
            max_chunk_size: Maximum chunk size in characters
            
        Returns:
            List of tuples: (chunk_text, position_map, chunk_id)
        """
        chunks = []
        current_chunk_parts = []
        current_position_map = []
        current_chunk_pos = 0
        chunk_counter = 0
        
        for idx, row_data in enumerate(rows_data):
            row_idx = start_row_idx + idx
            row_parts = []
            row_position_map = []
            row_local_pos = 0
            
            for field_name in filtered_fields:
                if field_name not in row_data:
                    continue
                
                # Safe string conversion
                try:
                    field_value = str(row_data[field_name]) if row_data[field_name] is not None else ""
                except Exception as e:
                    print(f"Warning: Failed to convert field '{field_name}' value to string in row {row_idx}: {e}")
                    continue
                
                # Skip empty values
                if not field_value.strip():
                    continue
                
                # Build field text: "field_name:value"
                field_text = f"{field_name}{self.field_value_separator}{field_value}"
                
                # Calculate positions RELATIVE to this row's start
                full_start = row_local_pos
                value_start = row_local_pos + len(field_name) + len(self.field_value_separator)
                value_end = value_start + len(field_value)
                full_end = row_local_pos + len(field_text)
                
                # Store position mapping (will adjust to chunk position later)
                row_position_map.append({
                    'row_idx': row_idx,
                    'field_name': field_name,
                    'value_start': value_start,
                    'value_end': value_end,
                    'full_start': full_start,
                    'full_end': full_end,
                    'chunk_id': f"chunk_{chunk_counter}"
                })
                
                row_parts.append(field_text)
                row_local_pos = full_end + len(self.field_separator)
            
            # Skip empty rows
            if not row_parts:
                continue
            
            # Join row fields with field separator
            row_text = self.field_separator.join(row_parts)
            
            # Calculate size if we add this row
            separator_size = len(self.row_separator) if current_chunk_parts else 0
            estimated_size = current_chunk_pos + separator_size + len(row_text)
            
            if current_chunk_parts and estimated_size > max_chunk_size:
                # Finalize current chunk
                chunk_text = self.row_separator.join(current_chunk_parts)
                chunk_id = f"chunk_{chunk_counter}"
                chunks.append((chunk_text, current_position_map, chunk_id))
                
                # Start new chunk
                chunk_counter += 1
                current_chunk_parts = [row_text]
                
                # Adjust positions for new chunk (starting at 0)
                adjusted_map = []
                for pos_dict in row_position_map:
                    adjusted_map.append(FieldPositionMap(
                        row_idx=pos_dict['row_idx'],
                        field_name=pos_dict['field_name'],
                        value_start=pos_dict['value_start'],
                        value_end=pos_dict['value_end'],
                        full_start=pos_dict['full_start'],
                        full_end=pos_dict['full_end'],
                        chunk_id=f"chunk_{chunk_counter}"
                    ))
                current_position_map = adjusted_map
                current_chunk_pos = len(row_text)
            else:
                # Add to current chunk
                # Adjust positions to account for current chunk content
                position_offset = current_chunk_pos + (len(self.row_separator) if current_chunk_parts else 0)
                
                for pos_dict in row_position_map:
                    current_position_map.append(FieldPositionMap(
                        row_idx=pos_dict['row_idx'],
                        field_name=pos_dict['field_name'],
                        value_start=pos_dict['value_start'] + position_offset,
                        value_end=pos_dict['value_end'] + position_offset,
                        full_start=pos_dict['full_start'] + position_offset,
                        full_end=pos_dict['full_end'] + position_offset,
                        chunk_id=f"chunk_{chunk_counter}"
                    ))
                
                current_chunk_parts.append(row_text)
                current_chunk_pos += len(row_text) + (len(self.row_separator) if len(current_chunk_parts) > 1 else 0)
        
        # Finalize last chunk if not empty
        if current_chunk_parts:
            chunk_text = self.row_separator.join(current_chunk_parts)
            chunk_id = f"chunk_{chunk_counter}"
            chunks.append((chunk_text, current_position_map, chunk_id))
        
        return chunks


    def build_multirow_text_no_names(
        self,
        rows_data: List[Dict[str, Any]],
        start_row_idx: int,
        filtered_fields: List[str],
        max_chunk_size: int = MAX_CHUNK_SIZE
    ) -> List[Tuple[str, List[FieldPositionMap], str]]:
        """
        Build text for MULTIPLE rows WITHOUT field names, chunked (Strategy #4)
        
        Format: "val1 | val2 || val3 | val4"
        Row separator: " || "
        
        Args:
            rows_data: List of row dictionaries
            start_row_idx: Starting row index from source table
            filtered_fields: List of fields to include
            max_chunk_size: Maximum chunk size in characters
            
        Returns:
            List of tuples: (chunk_text, position_map, chunk_id)
        """
        chunks = []
        current_chunk_parts = []
        current_position_map = []
        current_chunk_pos = 0
        chunk_counter = 0
        
        for idx, row_data in enumerate(rows_data):
            row_idx = start_row_idx + idx
            row_parts = []
            row_position_map = []
            row_local_pos = 0
            
            for field_name in filtered_fields:
                if field_name not in row_data:
                    continue
                
                # Safe string conversion
                try:
                    field_value = str(row_data[field_name]) if row_data[field_name] is not None else ""
                except Exception as e:
                    print(f"Warning: Failed to convert field '{field_name}' value to string in row {row_idx}: {e}")
                    continue
                
                # Skip empty values
                if not field_value.strip():
                    continue
                
                # Calculate positions (no field name, just value)
                value_start = row_local_pos
                value_end = row_local_pos + len(field_value)
                full_start = value_start
                full_end = value_end
                
                # Store position mapping (will adjust to chunk position later)
                row_position_map.append({
                    'row_idx': row_idx,
                    'field_name': field_name,
                    'value_start': value_start,
                    'value_end': value_end,
                    'full_start': full_start,
                    'full_end': full_end,
                    'chunk_id': f"chunk_{chunk_counter}"
                })
                
                row_parts.append(field_value)
                row_local_pos = value_end + len(self.field_separator)
            
            # Skip empty rows
            if not row_parts:
                continue
            
            # Join row fields with field separator
            row_text = self.field_separator.join(row_parts)
            
            # Calculate size if we add this row
            separator_size = len(self.row_separator) if current_chunk_parts else 0
            estimated_size = current_chunk_pos + separator_size + len(row_text)
            
            if current_chunk_parts and estimated_size > max_chunk_size:
                # Finalize current chunk
                chunk_text = self.row_separator.join(current_chunk_parts)
                chunk_id = f"chunk_{chunk_counter}"
                chunks.append((chunk_text, current_position_map, chunk_id))
                
                # Start new chunk
                chunk_counter += 1
                current_chunk_parts = [row_text]
                
                # Adjust positions for new chunk (starting at 0)
                adjusted_map = []
                for pos_dict in row_position_map:
                    adjusted_map.append(FieldPositionMap(
                        row_idx=pos_dict['row_idx'],
                        field_name=pos_dict['field_name'],
                        value_start=pos_dict['value_start'],
                        value_end=pos_dict['value_end'],
                        full_start=pos_dict['full_start'],
                        full_end=pos_dict['full_end'],
                        chunk_id=f"chunk_{chunk_counter}"
                    ))
                current_position_map = adjusted_map
                current_chunk_pos = len(row_text)
            else:
                # Add to current chunk
                # Adjust positions to account for current chunk content
                position_offset = current_chunk_pos + (len(self.row_separator) if current_chunk_parts else 0)
                
                for pos_dict in row_position_map:
                    current_position_map.append(FieldPositionMap(
                        row_idx=pos_dict['row_idx'],
                        field_name=pos_dict['field_name'],
                        value_start=pos_dict['value_start'] + position_offset,
                        value_end=pos_dict['value_end'] + position_offset,
                        full_start=pos_dict['full_start'] + position_offset,
                        full_end=pos_dict['full_end'] + position_offset,
                        chunk_id=f"chunk_{chunk_counter}"
                    ))
                
                current_chunk_parts.append(row_text)
                current_chunk_pos += len(row_text) + (len(self.row_separator) if len(current_chunk_parts) > 1 else 0)
        
        # Finalize last chunk if not empty
        if current_chunk_parts:
            chunk_text = self.row_separator.join(current_chunk_parts)
            chunk_id = f"chunk_{chunk_counter}"
            chunks.append((chunk_text, current_position_map, chunk_id))
        
        return chunks

    def build_column_chunks_with_mapping(
        self,
        column_values: List[Any],
        field_name: str,
        start_row_idx: int = 0,
        max_chunk_size: int = MAX_CHUNK_SIZE
    ) -> List[Tuple[str, List[FieldPositionMap], str]]:
        """
        Build chunks for single column with row tracking (Strategy #5)
        
        Format: "value1 | value2 | value3 | ..."
        
        Truncates individual values > MAX_VALUE_SIZE
        Chunks when total size exceeds max_chunk_size
        
        Args:
            column_values: List of all values for one column (up to 30K rows)
            field_name: The column name
            start_row_idx: Starting row index from source table
            max_chunk_size: Maximum chunk size in characters
            
        Returns:
            List of tuples: (chunk_text, position_map, chunk_id)
        """
        chunks = []
        current_chunk_parts = []
        current_position_map = []
        current_chunk_pos = 0
        chunk_counter = 0
        truncated_count = 0
        
        for idx, value in enumerate(column_values):
            row_idx = start_row_idx + idx
            
            # Safe string conversion
            try:
                str_value = str(value) if value is not None else ""
            except Exception as e:
                print(f"Warning: Failed to convert value to string for column '{field_name}' at row {row_idx}: {e}")
                continue
            
            # Skip empty values
            if not str_value.strip():
                continue
            
            # Truncate if value exceeds MAX_VALUE_SIZE
            is_truncated = False
            if len(str_value) > MAX_VALUE_SIZE:
                str_value = str_value[:MAX_VALUE_SIZE]
                is_truncated = True
                truncated_count += 1
                print(f"Warning: Truncated value for column '{field_name}' at row {row_idx} from {len(str(value))} to {MAX_VALUE_SIZE} chars")
            
            # Calculate size if we add this value
            separator_size = len(self.field_separator) if current_chunk_parts else 0
            estimated_size = current_chunk_pos + separator_size + len(str_value)
            
            if current_chunk_parts and estimated_size > max_chunk_size:
                # Finalize current chunk
                chunk_text = self.field_separator.join(current_chunk_parts)
                chunk_id = f"col_{field_name}_chunk_{chunk_counter}"
                chunks.append((chunk_text, current_position_map, chunk_id))
                
                # Start new chunk
                chunk_counter += 1
                current_chunk_parts = [str_value]
                
                # New chunk starts at position 0
                current_position_map = [FieldPositionMap(
                    row_idx=row_idx,
                    field_name=field_name,
                    value_start=0,
                    value_end=len(str_value),
                    full_start=0,
                    full_end=len(str_value),
                    chunk_id=f"col_{field_name}_chunk_{chunk_counter}"
                )]
                current_chunk_pos = len(str_value)
            else:
                # Add to current chunk
                position_offset = current_chunk_pos + (len(self.field_separator) if current_chunk_parts else 0)
                
                value_start = position_offset
                value_end = position_offset + len(str_value)
                
                current_position_map.append(FieldPositionMap(
                    row_idx=row_idx,
                    field_name=field_name,
                    value_start=value_start,
                    value_end=value_end,
                    full_start=value_start,
                    full_end=value_end,
                    chunk_id=f"col_{field_name}_chunk_{chunk_counter}"
                ))
                
                current_chunk_parts.append(str_value)
                current_chunk_pos += len(str_value) + (len(self.field_separator) if len(current_chunk_parts) > 1 else 0)
        
        # Finalize last chunk if not empty
        if current_chunk_parts:
            chunk_text = self.field_separator.join(current_chunk_parts)
            chunk_id = f"col_{field_name}_chunk_{chunk_counter}"
            chunks.append((chunk_text, current_position_map, chunk_id))
        
        if truncated_count > 0:
            print(f"Info: Column '{field_name}' had {truncated_count} truncated values")
        
        return chunks


    def extract_source_location(
        self,
        finding_start: int,
        finding_end: int,
        position_map: List[FieldPositionMap]
    ) -> Optional[Tuple[int, str]]:
        """
        Map finding position to source location
        
        Args:
            finding_start: Start position in chunk text
            finding_end: End position in chunk text
            position_map: Position mappings for this chunk
            
        Returns:
            Tuple of (row_idx, field_name) or None if not found
        """
        try:
            for pos_map in position_map:
                # Check if finding is within this field's value area (exact match)
                if (finding_start >= pos_map.value_start and
                    finding_end <= pos_map.value_end):
                    return (pos_map.row_idx, pos_map.field_name)
                
                # Check for overlap (partial matches)
                if (finding_start < pos_map.value_end and
                    finding_end > pos_map.value_start):
                    return (pos_map.row_idx, pos_map.field_name)
            
            # No mapping found
            print(f"Warning: Could not map finding at position {finding_start}-{finding_end} to source location")
            return None
            
        except Exception as e:
            print(f"Error: Failed to extract source location for finding at {finding_start}-{finding_end}: {e}")
            return None

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
    Filter table columns to include only those likely to contain PII,
    now with detailed logging for debugging purposes.
    """
    #print("\n" + "-"*20 + " Running Field Filter " + "-"*20)
    included_fields = []
    excluded_count = 0
    
    # Ensure there are columns to iterate over
    columns = table_metadata.get('columns', {})
    if not columns:
        print("Warning: No columns found in table metadata.")
    
    for column_name, column_info in columns.items():
        data_type = column_info.get('data_type', '').upper()
        
        #print(f"Evaluating column: '{column_name}' (Type: {data_type})")

        # Step 1: Data type filter for non-textual types
        if data_type in ['DATE', 'TIMESTAMP', 'BOOLEAN', 'BINARY']:
            #print(f" -> EXCLUDED: Non-textual data type '{data_type}'.")
            excluded_count += 1
            continue

        # Step 2: Contextual filter for numeric types
        if data_type in ['INT', 'BIGINT', 'NUMERIC', 'DECIMAL']:
            column_lower = column_name.lower()
            
            pii_patterns = [
                'account', 'passport', 'license', 'ssn', 'social',
                'credit_card', 'card', 'number', 'id_number'
            ]
            
            generic_patterns = [
                'customer_id', 'order_id', 'item_id', 'user_id',
                'quantity', 'age', 'amount', 'price', 'count'
            ]
            
            if any(pattern in column_lower for pattern in generic_patterns):
                #print(f" -> EXCLUDED: Numeric column name matches a generic ID pattern.")
                excluded_count += 1
                continue
            
            if any(pattern in column_lower for pattern in pii_patterns):
                #print(f" -> INCLUDED: Numeric column name matches a PII pattern.")
                included_fields.append(column_name)
                continue  # Move to the next column
            else:
                #print(f" -> EXCLUDED: Numeric column with a non-PII name.")
                included_fields.append(column_name)
                continue
        
        # If it's not excluded by the rules above, it's included (e.g., VARCHAR, TEXT)
        #print(f" -> INCLUDED: General text-based data type.")
        included_fields.append(column_name)
    
    #print("--- Field Filter Summary ---")
    #print(f"Included fields for scanning: {included_fields}")
    #print(f"Excluded fields: {excluded_count}")
    #print("-" * 62 + "\n")
    
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