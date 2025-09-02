# classification/row_processor.py
"""
Row-level processing for structured data following Presidio best practices.
Processes database rows and documents with field-level tracking.
"""

from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timezone
import asyncio

from .field_mapper import FieldMapper, filter_fields_for_classification
from core.models import PIIFinding


class RowProcessor:
    """Processes database rows following Presidio best practices"""
    
    def __init__(self):
        self.field_mapper = FieldMapper()
    
    async def process_database_row(self,
                                  row_data: Dict[str, Any],
                                  row_pk: Dict[str, Any],
                                  table_metadata: Dict[str, Any],
                                  classifier) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process single database row with field-level tracking
        
        Args:
            row_data: Dictionary of column_name -> value
            row_pk: Primary key values for this row
            table_metadata: Table schema information
            classifier: Classification engine instance
            
        Returns:
            Dictionary mapping field_name -> list of findings
        """
        try:
            # Filter fields for classification
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            # Build combined text with position mapping
            combined_text, position_map = self.field_mapper.build_row_text_with_mapping(
                row_data, filtered_fields
            )
            
            # Skip empty rows
            if not combined_text.strip():
                return {}
            
            # Prepare context for classification
            context_info = {
                "object_type": "database_row",
                "table_name": table_metadata.get('table_name'),
                "schema_name": table_metadata.get('schema_name'),
                "database_name": table_metadata.get('database_name'),
                "row_identifier": row_pk,
                "row_data": row_data,
                "table_metadata": table_metadata,
                "position_map": position_map
            }
            
            # Run classification
            findings = classifier.classify_content(combined_text, context_info)
            
            # Map findings back to fields
            field_findings = self.field_mapper.validate_field_mapping(findings, position_map)
            
            # Convert to structured format
            structured_findings = {}
            for field_name, field_findings_list in field_findings.items():
                structured_findings[field_name] = [
                    {
                        "entity_type": finding.entity_type,
                        "text": finding.text,
                        "confidence": finding.confidence_score,
                        "row_identifier": row_pk,
                        "start_position": finding.start_position,
                        "end_position": finding.end_position,
                        "detection_timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    for finding in field_findings_list
                ]
            #str_str = json.dumps(structured_findings, indent=4)
            #print(str_str)
            #tblme_str = json.dumps(table_metadata, indent=4)
            #print(tblme_str)

            
            return structured_findings
            
        except Exception as e:
            # Return empty results on error, log for debugging
            print(f"Error processing row {row_pk}: {str(e)}")
            return {}
    
    async def process_document_content(self,
                                     content: str,
                                     file_metadata: Dict[str, Any],
                                     classifier) -> List[Dict[str, Any]]:
        """
        Process document content for PII detection
        
        Args:
            content: Document text content
            file_metadata: File context information
            classifier: Classification engine instance
            
        Returns:
            List of findings for the "default" field
        """
        try:
            # Prepare context for classification
            context_info = {
                "object_type": "document",
                "file_path": file_metadata.get('file_path'),
                "file_name": file_metadata.get('file_name'),
                "file_size": file_metadata.get('file_size', 0),
                "file_extension": file_metadata.get('file_extension'),
                "content_extracted_size": len(content)
            }
            
            # Run classification
            findings = classifier.classify_content(content, context_info)
            
            # Convert to structured format
            structured_findings = [
                {
                    "entity_type": finding.entity_type,
                    "text": finding.text,
                    "confidence": finding.confidence_score,
                    "start_position": finding.start_position,
                    "end_position": finding.end_position,
                    "detection_timestamp": datetime.now(timezone.utc).isoformat()
                }
                for finding in findings
            ]
            
            return structured_findings
            
        except Exception as e:
            print(f"Error processing document {file_metadata.get('file_path')}: {str(e)}")
            return []


def aggregate_row_results(all_row_results: List[Dict[str, List]],
                         table_metadata: Dict[str, Any],
                         max_samples: int = 100) -> Dict[str, Any]:
    """
    Aggregate row-level results into field-level statistics
    
    Args:
        all_row_results: List of row processing results
        table_metadata: Table schema information
        max_samples: Maximum number of sample identifiers to keep
        
    Returns:
        Aggregated field-level results
    """
    field_aggregates = {}
    
    # Process each row's results
    for row_result in all_row_results:
        for field_name, findings in row_result.items():
            if field_name not in field_aggregates:
                field_aggregates[field_name] = {
                    'all_findings': [],
                    'entity_types': set(),
                    'confidence_scores': [],
                    'row_identifiers': []
                }
            
            # Aggregate findings
            field_aggregates[field_name]['all_findings'].extend(findings)
            for finding in findings:
                field_aggregates[field_name]['entity_types'].add(finding['entity_type'])
                field_aggregates[field_name]['confidence_scores'].append(finding['confidence'])
                field_aggregates[field_name]['row_identifiers'].append({
                    **finding['row_identifier'],
                    'confidence': finding['confidence']
                })
    
    # Calculate final statistics
    final_results = {}
    for field_name, aggregates in field_aggregates.items():
        # Sort by confidence (highest first)
        sorted_identifiers = sorted(
            aggregates['row_identifiers'],
            key=lambda x: x['confidence'],
            reverse=True
        )
        
        # Take top N samples
        sample_identifiers = sorted_identifiers[:max_samples]
        
        # Calculate statistics
        confidence_scores = aggregates['confidence_scores']
        final_results[field_name] = {
            'total_findings': len(aggregates['all_findings']),
            'unique_entity_types': list(aggregates['entity_types']),
            'sample_identifiers': sample_identifiers,
            'confidence_statistics': {
                'average': sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0,
                'max': max(confidence_scores) if confidence_scores else 0,
                'min': min(confidence_scores) if confidence_scores else 0
            },
            'sample_count': len(sample_identifiers),
            'total_rows_with_findings': len(set(
                str(rid) for rid in aggregates['row_identifiers']
            ))
        }
    
    return final_results


def calculate_field_statistics(field_findings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics for a single field
    
    Args:
        field_findings: List of findings for a specific field
        
    Returns:
        Statistical summary for the field
    """
    if not field_findings:
        return {
            'total_findings': 0,
            'confidence_statistics': {'average': 0, 'max': 0, 'min': 0},
            'entity_type_distribution': {},
            'temporal_distribution': {}
        }
    
    # Basic counts
    total_findings = len(field_findings)
    
    # Confidence statistics
    confidences = [f['confidence'] for f in field_findings]
    confidence_stats = {
        'average': sum(confidences) / len(confidences),
        'max': max(confidences),
        'min': min(confidences),
        'high_confidence_count': len([c for c in confidences if c >= 0.8]),
        'medium_confidence_count': len([c for c in confidences if 0.6 <= c < 0.8]),
        'low_confidence_count': len([c for c in confidences if c < 0.6])
    }
    
    # Entity type distribution
    entity_types = [f['entity_type'] for f in field_findings]
    entity_distribution = {}
    for entity_type in set(entity_types):
        entity_distribution[entity_type] = entity_types.count(entity_type)
    
    # Temporal distribution (by hour of detection)
    temporal_distribution = {}
    for finding in field_findings:
        try:
            timestamp = finding.get('detection_timestamp', '')
            if timestamp:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                hour_key = f"{dt.hour:02d}:00"
                temporal_distribution[hour_key] = temporal_distribution.get(hour_key, 0) + 1
        except Exception:
            # Skip malformed timestamps
            continue
    
    return {
        'total_findings': total_findings,
        'confidence_statistics': confidence_stats,
        'entity_type_distribution': entity_distribution,
        'temporal_distribution': temporal_distribution,
        'unique_row_count': len(set(str(f.get('row_identifier', '')) for f in field_findings))
    }


def validate_row_processing_results(results: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Validate row processing results for consistency
    
    Args:
        results: Row processing results
        
    Returns:
        Validation report
    """
    validation_report = {
        'is_valid': True,
        'issues': [],
        'field_count': len(results),
        'total_findings': 0,
        'fields_with_findings': 0
    }
    
    for field_name, findings in results.items():
        field_findings_count = len(findings)
        validation_report['total_findings'] += field_findings_count
        
        if field_findings_count > 0:
            validation_report['fields_with_findings'] += 1
            
            # Validate each finding
            for i, finding in enumerate(findings):
                # Check required fields
                required_fields = ['entity_type', 'text', 'confidence', 'row_identifier']
                for req_field in required_fields:
                    if req_field not in finding:
                        validation_report['issues'].append(
                            f"Field '{field_name}' finding {i} missing required field '{req_field}'"
                        )
                        validation_report['is_valid'] = False
                
                # Validate confidence range
                confidence = finding.get('confidence', 0)
                if not 0.0 <= confidence <= 1.0:
                    validation_report['issues'].append(
                        f"Field '{field_name}' finding {i} has invalid confidence: {confidence}"
                    )
                    validation_report['is_valid'] = False
                
                # Validate positions
                start_pos = finding.get('start_position', 0)
                end_pos = finding.get('end_position', 0)
                if start_pos < 0 or end_pos < start_pos:
                    validation_report['issues'].append(
                        f"Field '{field_name}' finding {i} has invalid positions: {start_pos}-{end_pos}"
                    )
                    validation_report['is_valid'] = False
    
    return validation_report


# Test functions
async def test_row_processor():
    """Test row processing functionality"""
    processor = RowProcessor()
    
    # Mock classifier
    class MockClassifier:
        def classify_content(self, content, context):
            # Return mock findings for testing
            findings = []
            if "john.doe@example.com" in content:
                finding = type('Finding', (), {
                    'entity_type': 'EMAIL_ADDRESS',
                    'text': 'john.doe@example.com',
                    'confidence_score': 0.95,
                    'start_position': content.find('john.doe@example.com'),
                    'end_position': content.find('john.doe@example.com') + len('john.doe@example.com')
                })()
                findings.append(finding)
            return findings
    
    # Test data
    row_data = {
        "customer_id": 1001,
        "customer_name": "John Doe",
        "email": "john.doe@example.com",
        "phone": "555-123-4567"
    }
    
    row_pk = {"customer_id": 1001}
    
    table_metadata = {
        "table_name": "customers",
        "schema_name": "dbo", 
        "database_name": "testdb",
        "columns": {
            "customer_id": {"data_type": "int"},
            "customer_name": {"data_type": "varchar"},
            "email": {"data_type": "varchar"},
            "phone": {"data_type": "varchar"}
        }
    }
    
    # Test row processing
    classifier = MockClassifier()
    results = await processor.process_database_row(row_data, row_pk, table_metadata, classifier)
    
    print(f"Row processing results: {results}")
    
    # Validate results
    validation_report = validate_row_processing_results(results)
    print(f"Validation report: {validation_report}")
    
    # Test aggregation
    all_results = [results]  # Single row for testing
    aggregated = aggregate_row_results(all_results, table_metadata)
    print(f"Aggregated results: {aggregated}")
    
    assert validation_report['is_valid'], f"Validation failed: {validation_report['issues']}"
    print("✅ Row processor test passed!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_row_processor())