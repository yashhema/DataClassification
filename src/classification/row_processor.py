# classification/row_processor.py
"""
Row-level processing for structured data following Presidio best practices.
Processes database rows and documents with field-level tracking.
"""

from typing import Dict, List, Any, Optional, Set, Tuple  
from collections import defaultdict
from datetime import datetime, timezone
import asyncio

from .field_mapper import FieldMapper, filter_fields_for_classification, FieldPositionMap 
from .generic_enums import ProcessingStrategy
from core.models.models import PIIFinding
from core.models.stats_models import ColumnStatistics, MatchStatistics, ConfidenceComponents
from core.errors import ProcessingError, ErrorType

# Configuration constants
MAX_CHUNK_SIZE = 45000
MAX_VALUE_SIZE = 50000
MAX_ROWS_PER_COLUMN = 30000
FIELD_SEPARATOR = " | "
FIELD_VALUE_SEPARATOR = ":"
ROW_SEPARATOR = " || "

class RowProcessor:
    """Processes database rows following Presidio best practices"""
    
    def __init__(self):
        """
        Initialize DataProcessor with field mapper and statistics caches
        """
        self.field_mapper = FieldMapper()
        self.strategy: Optional[ProcessingStrategy] = None
        
        # Statistics caches
        self.column_stats_cache: Dict[str, ColumnStatistics] = {}
        self.match_stats_cache: Dict[str, MatchStatistics] = {}


    def _calculate_column_statistics(
        self,
        column_values: List[Any],
        field_name: str
    ) -> ColumnStatistics:
        """
        Calculate statistics for single column
        
        Args:
            column_values: List of all values for one column
            field_name: The column name
            
        Returns:
            ColumnStatistics object with calculated metrics
        """
        try:
            total_rows = len(column_values)
            
            # Null statistics
            null_count = sum(1 for v in column_values if v is None)
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            # Filter non-null values for further analysis
            non_null_values = [v for v in column_values if v is not None]
            
            if not non_null_values:
                # All nulls - return minimal stats
                return ColumnStatistics(
                    field_name=field_name,
                    total_rows_scanned=total_rows,
                    null_count=null_count,
                    null_percentage=null_percentage,
                    min_value_length=0,
                    max_value_length=0,
                    mean_value_length=0.0,
                    distinct_value_count=0,
                    distinct_value_percentage=0.0,
                    most_common_value=None,
                    most_common_value_count=0,
                    most_common_value_percentage=0.0,
                    has_truncated_values=False,
                    truncated_value_count=0
                )
            
            # Length statistics
            lengths = []
            truncated_count = 0
            
            for v in non_null_values:
                try:
                    str_val = str(v)
                    lengths.append(len(str_val))
                    if len(str_val) > MAX_VALUE_SIZE:
                        truncated_count += 1
                except Exception as e:
                    print(f"Warning: Failed to convert value to string in column '{field_name}': {e}")
                    continue
            
            min_length = min(lengths) if lengths else 0
            max_length = max(lengths) if lengths else 0
            mean_length = sum(lengths) / len(lengths) if lengths else 0.0
            
            # Uniqueness statistics
            str_values = []
            for v in non_null_values:
                try:
                    str_values.append(str(v))
                except Exception:
                    continue
            
            distinct_values = set(str_values)
            distinct_count = len(distinct_values)
            distinct_percentage = (distinct_count / total_rows * 100) if total_rows > 0 else 0
            
            # Most common value
            most_common_value = None
            most_common_count = 0
            most_common_percentage = 0.0
            
            if str_values:
                value_counts = {}
                for val in str_values:
                    value_counts[val] = value_counts.get(val, 0) + 1
                
                most_common_value = max(value_counts, key=value_counts.get)
                most_common_count = value_counts[most_common_value]
                most_common_percentage = (most_common_count / total_rows * 100) if total_rows > 0 else 0
            
            return ColumnStatistics(
                field_name=field_name,
                total_rows_scanned=total_rows,
                null_count=null_count,
                null_percentage=null_percentage,
                min_value_length=min_length,
                max_value_length=max_length,
                mean_value_length=mean_length,
                distinct_value_count=distinct_count,
                distinct_value_percentage=distinct_percentage,
                most_common_value=most_common_value,
                most_common_value_count=most_common_count,
                most_common_value_percentage=most_common_percentage,
                has_truncated_values=(truncated_count > 0),
                truncated_value_count=truncated_count
            )
            
        except Exception as e:
            print(f"Error: Failed to calculate statistics for column '{field_name}': {e}")
            # Return minimal stats on error
            return ColumnStatistics(
                field_name=field_name,
                total_rows_scanned=len(column_values),
                null_count=0,
                null_percentage=0.0,
                min_value_length=0,
                max_value_length=0,
                mean_value_length=0.0,
                distinct_value_count=0,
                distinct_value_percentage=0.0,
                most_common_value=None,
                most_common_value_count=0,
                most_common_value_percentage=0.0,
                has_truncated_values=False,
                truncated_value_count=0
            )


    def _calculate_all_column_statistics(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any]
    ):
        """
        Calculate ColumnStatistics for all filtered fields
        Stores results in self.column_stats_cache
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
        """
        try:
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            for field_name in filtered_fields:
                # Extract all values for this column
                column_values = [row.get(field_name) for row in table_data]
                
                # Calculate statistics
                stats = self._calculate_column_statistics(column_values, field_name)
                
                # Store in cache
                self.column_stats_cache[field_name] = stats
                
            print(f"Info: Calculated statistics for {len(filtered_fields)} columns")
            
        except Exception as e:
            print(f"Error: Failed to calculate column statistics: {e}")
            # Continue with empty cache - non-fatal error


    def _initialize_match_statistics(
        self,
        active_classifiers: List[Dict[str, Any]],
        table_metadata: Dict[str, Any]
    ):
        """
        Initialize MatchStatistics objects for all classifiers
        Stores in self.match_stats_cache with key: f"{classifier_id}:{field_name}"
        
        Args:
            active_classifiers: List of classifier configuration dictionaries
            table_metadata: Table schema information
        """
        try:
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            for classifier in active_classifiers:
                classifier_id = classifier.get('classifier_id')
                entity_type = classifier.get('entity_type')
                
                if not classifier_id or not entity_type:
                    print(f"Warning: Skipping classifier with missing id or entity_type")
                    continue
                
                # For columnar strategy: one MatchStatistics per (classifier, field)
                # For row-based strategy: one MatchStatistics per classifier (field_name = "multiple")
                if self.strategy == ProcessingStrategy.COLUMNAR:
                    for field_name in filtered_fields:
                        cache_key = f"{classifier_id}:{field_name}"
                        self.match_stats_cache[cache_key] = MatchStatistics(
                            classifier_id=classifier_id,
                            entity_type=entity_type,
                            field_name=field_name
                        )
                else:
                    # Row-based strategies: single entry per classifier
                    cache_key = f"{classifier_id}:multiple"
                    self.match_stats_cache[cache_key] = MatchStatistics(
                        classifier_id=classifier_id,
                        entity_type=entity_type,
                        field_name="multiple"
                    )
            
            print(f"Info: Initialized {len(self.match_stats_cache)} match statistics trackers")
            
        except Exception as e:
            print(f"Error: Failed to initialize match statistics: {e}")
            # Continue with empty cache - non-fatal error
    

    def _finalize_match_statistics(self, total_rows: int):
        """
        Finalize all match statistics (calculate percentages)
        Called after all findings processed
        
        Args:
            total_rows: Total number of rows processed
        """
        try:
            for cache_key, match_stats in self.match_stats_cache.items():
                match_stats.finalize(total_rows)
            
            print(f"Info: Finalized {len(self.match_stats_cache)} match statistics")
            
        except Exception as e:
            print(f"Error: Failed to finalize match statistics: {e}")
            # Non-fatal - statistics may be incomplete but processing continues


    def _apply_dictionary_boosts(
        self,
        finding: PIIFinding,
        row_idx: int,
        field_name: str,
        classifier_config: Dict[str, Any],
        table_data: List[Dict[str, Any]],
        chunk_text: str
    ) -> ConfidenceComponents:
        """
        Apply all dictionary-based confidence boosts
        
        Checks:
        1. column_names: Does field_name match keywords?
        2. exact_match: Are exact phrases in context?
        3. negative: Are negative keywords present?
        4. cross_column (simple rules): COL_NAME_MATCH, VALUE_MATCH_IN_COLUMN_VALUE
        
        Note: dictionary.words boost is NOT applied (already handled by Presidio)
        
        Args:
            finding: The PIIFinding object from Presidio
            row_idx: Source row index
            field_name: Source field name
            classifier_config: Classifier configuration with dictionary
            table_data: Full table data for cross-column lookups
            chunk_text: The text that was classified
            
        Returns:
            ConfidenceComponents with all boosts calculated
        """
        try:
            # Start with Presidio's base score (already includes words boost)
            components = ConfidenceComponents(
                presidio_base_score=finding.confidence_score
            )
            
            # Get dictionary from classifier config
            dictionary = classifier_config.get('dictionary')
            if not dictionary:
                # No dictionary - return base score
                components.final_confidence = finding.confidence_score
                return components
            
            # Extract context window around finding for context-based checks
            context_window_size = 100
            context_start = max(0, finding.start_position - context_window_size)
            context_end = min(len(chunk_text), finding.end_position + context_window_size)
            context_window = chunk_text[context_start:context_end]
            
            # 1. Column name boost
            if dictionary.get('column_names'):
                column_names_rule = dictionary['column_names']
                keywords = column_names_rule.get('vallst', [])
                boost = column_names_rule.get('boost', 0)
                
                if keywords and any(kw.lower() in field_name.lower() for kw in keywords):
                    components.column_name_boost = boost
            
            # 2. Exact match boost
            if dictionary.get('exact_match'):
                exact_match_rule = dictionary['exact_match']
                phrases = exact_match_rule.get('vallst', [])
                boost = exact_match_rule.get('boost', 0)
                
                if phrases:
                    for phrase in phrases:
                        if phrase.lower() in context_window.lower():
                            components.exact_match_boost = boost
                            break  # Apply boost once even if multiple matches
            
            # 3. Negative penalty
            if dictionary.get('negative'):
                negative_rule = dictionary['negative']
                terms = negative_rule.get('vallst', [])
                penalty = negative_rule.get('boost', 0)  # This should be negative value
                
                if terms:
                    for term in terms:
                        if term.lower() in context_window.lower():
                            components.negative_penalty = penalty
                            break  # Apply penalty once even if multiple matches
            
            # 4. Cross-column simple rules (if enabled)
            if dictionary.get('cross_column_support', {}).get('enabled'):
                rules = dictionary['cross_column_support'].get('rules', [])
                if rules and row_idx < len(table_data):
                    row_data = table_data[row_idx]
                    components.cross_column_boost = self._apply_cross_column_simple_rules(
                        rules, row_data, field_name
                    )
            
            # Calculate final confidence (clamped to [0.0, 1.0])
            components.calculate_final()
            
            return components
            
        except Exception as e:
            print(f"Error: Failed to apply dictionary boosts for finding at row {row_idx}, field '{field_name}': {e}")
            # Return base score on error
            return ConfidenceComponents(
                presidio_base_score=finding.confidence_score,
                final_confidence=finding.confidence_score
            )


    def _apply_cross_column_simple_rules(
        self,
        rules: List[Dict[str, Any]],
        row_data: Dict[str, Any],
        current_field_name: str
    ) -> float:
        """
        Apply cross-column simple rules (COL_NAME_MATCH, VALUE_MATCH_IN_COLUMN_VALUE)
        
        Note: CATEGORY_CO_OCCURRENCE is deferred and handled separately
        
        Args:
            rules: List of cross-column rule dictionaries
            row_data: The complete row data
            current_field_name: The field where the finding was detected
            
        Returns:
            Total boost value (max one boost even if multiple rules match)
        """
        try:
            max_boost = 0.0
            
            for rule in rules:
                rule_type = rule.get('rule_type')
                
                # Skip CATEGORY_CO_OCCURRENCE (handled separately)
                if rule_type == 'CATEGORY_CO_OCCURRENCE':
                    continue
                
                # 1. COL_NAME_MATCH: Check if keywords in any column names
                if rule_type == 'COL_NAME_MATCH':
                    parameters = rule.get('parameters', {})
                    keywords = parameters.get('keywords', [])
                    boost = parameters.get('confidence_boost', 0)
                    
                    if keywords:
                        # Check all column names in the row
                        for col_name in row_data.keys():
                            if any(kw.lower() in col_name.lower() for kw in keywords):
                                max_boost = max(max_boost, boost)
                                break  # Found match, stop checking columns
                
                # 2. VALUE_MATCH_IN_COLUMN_VALUE: Check if keywords in any column values
                elif rule_type == 'VALUE_MATCH_IN_COLUMN_VALUE':
                    parameters = rule.get('parameters', {})
                    keywords = parameters.get('keywords', [])
                    
                    if keywords:
                        # Check all column values in the row
                        for col_name, col_value in row_data.items():
                            # Skip the current field (already matched)
                            if col_name == current_field_name:
                                continue
                            
                            # Convert value to string safely
                            try:
                                str_value = str(col_value) if col_value is not None else ""
                            except Exception:
                                continue
                            
                            # Check if any keyword matches
                            for keyword_obj in keywords:
                                # keyword_obj is ValueMatchKeyword with 'value' and 'boost'
                                keyword_value = keyword_obj.get('value', '')
                                keyword_boost = keyword_obj.get('boost', 0)
                                
                                if keyword_value.lower() in str_value.lower():
                                    max_boost = max(max_boost, keyword_boost)
                                    break  # Found match, stop checking keywords
                            
                            if max_boost > 0:
                                break  # Found match, stop checking columns
            
            return max_boost
            
        except Exception as e:
            print(f"Error: Failed to apply cross-column simple rules: {e}")
            return 0.0

    def _apply_category_cooccurrence_boosts(
        self,
        all_findings: List[PIIFinding],
        table_data: List[Dict[str, Any]],
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Apply CATEGORY_CO_OCCURRENCE boosts after all findings collected
        
        This is deferred processing because it requires seeing ALL findings
        in a row before applying boosts based on which entity types are present.
        
        Steps:
        1. Group findings by row_idx
        2. For each classifier with CATEGORY_CO_OCCURRENCE rules:
           a. Check which target categories present in row
           b. Apply boost based on boost_tiers
           c. Update finding confidence
        
        Args:
            all_findings: All findings from processing
            table_data: Full table data
            active_classifiers: List of classifier configurations
            
        Returns:
            Updated list of findings with co-occurrence boosts applied
        """
        try:
            # Group findings by row_idx
            findings_by_row: Dict[int, List[PIIFinding]] = defaultdict(list)
            for finding in all_findings:
                row_idx = finding.context_data.get('row_idx')
                if row_idx is not None:
                    findings_by_row[row_idx].append(finding)
            
            # Check which classifiers have CATEGORY_CO_OCCURRENCE rules
            classifiers_with_cooccurrence = []
            for classifier in active_classifiers:
                dictionary = classifier.get('dictionary')
                if dictionary and dictionary.get('cross_column_support', {}).get('enabled'):
                    rules = dictionary['cross_column_support'].get('rules', [])
                    cooccurrence_rules = [r for r in rules if r.get('rule_type') == 'CATEGORY_CO_OCCURRENCE']
                    if cooccurrence_rules:
                        classifiers_with_cooccurrence.append({
                            'classifier_id': classifier.get('classifier_id'),
                            'entity_type': classifier.get('entity_type'),
                            'rules': cooccurrence_rules
                        })
            
            if not classifiers_with_cooccurrence:
                # No co-occurrence rules to apply
                return all_findings
            
            # Apply co-occurrence boosts
            for row_idx, findings_in_row in findings_by_row.items():
                # Get entity types present in this row
                entity_types_in_row = set(f.entity_type for f in findings_in_row)
                
                # For each classifier with co-occurrence rules
                for classifier_info in classifiers_with_cooccurrence:
                    classifier_id = classifier_info['classifier_id']
                    
                    # Find findings from this classifier in this row
                    classifier_findings = [f for f in findings_in_row if f.classifier_id == classifier_id]
                    
                    if not classifier_findings:
                        continue
                    
                    # Check each co-occurrence rule
                    for rule in classifier_info['rules']:
                        parameters = rule.get('parameters', {})
                        target_categories = parameters.get('target_categories', [])
                        boost_tiers = parameters.get('boost_tiers', [])
                        
                        if not target_categories or not boost_tiers:
                            continue
                        
                        # Count how many target categories are present
                        categories_found = sum(1 for cat in target_categories if cat in entity_types_in_row)
                        
                        # Find appropriate boost tier
                        applicable_boost = 0.0
                        for tier in boost_tiers:
                            tier_categories_found = tier.get('categories_found', 0)
                            tier_boost = tier.get('confidence_boost', 0)
                            
                            if categories_found >= tier_categories_found:
                                applicable_boost = max(applicable_boost, tier_boost)
                        
                        # Apply boost to all findings from this classifier in this row
                        if applicable_boost > 0:
                            for finding in classifier_findings:
                                # Get existing confidence components
                                confidence_components = finding.context_data.get('confidence_components', {})
                                
                                # Add co-occurrence boost
                                current_cross_column = confidence_components.get('cross_column_boost', 0)
                                confidence_components['cross_column_boost'] = current_cross_column + applicable_boost
                                
                                # Recalculate final confidence
                                final_confidence = min(1.0, max(0.0,
                                    confidence_components.get('presidio_base_score', 0) +
                                    confidence_components.get('column_name_boost', 0) +
                                    confidence_components.get('words_boost', 0) +
                                    confidence_components.get('exact_match_boost', 0) +
                                    confidence_components.get('negative_penalty', 0) +
                                    confidence_components.get('cross_column_boost', 0)
                                ))
                                
                                confidence_components['final_confidence'] = final_confidence
                                finding.context_data['confidence_components'] = confidence_components
                                finding.confidence_score = final_confidence
            
            print(f"Info: Applied CATEGORY_CO_OCCURRENCE boosts to {len(all_findings)} findings across {len(findings_by_row)} rows")
            
            return all_findings
            
        except Exception as e:
            print(f"Error: Failed to apply category co-occurrence boosts: {e}")
            # Return unmodified findings on error
            return all_findings


    async def _process_chunk(
        self,
        chunk_text: str,
        position_map: List[FieldPositionMap],
        chunk_id: str,
        context_info: Dict[str, Any],
        table_data: List[Dict[str, Any]],
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Process single chunk through classification engine.
        
        UPDATED: Now calculates weighted confidence with proper formula.
        """
        try:
            processed_findings = []
            
            # Update context_info with chunk_id
            context_info['chunk_id'] = chunk_id
            
            # Run classification through engine
            loop = asyncio.get_running_loop()
            raw_findings = await loop.run_in_executor(
                None,
                classifier_engine.classify_content,
                chunk_text,
                context_info
            )
            
            # Determine if this is structured data
            is_structured = context_info.get('object_type') in ['database_row', 'database_column', 'database_multirow']
            
            # Process each finding
            for finding in raw_findings:
                # 1. Map finding to source location
                source_location = self.field_mapper.extract_source_location(
                    finding.start_position,
                    finding.end_position,
                    position_map
                )
                
                if source_location is None:
                    print(f"Warning: Could not map finding at position {finding.start_position}-{finding.end_position} in chunk '{chunk_id}'")
                    continue
                
                row_idx, field_name = source_location
                
                # 2. Get classifier config
                classifier_config = None
                for classifier in active_classifiers:
                    if classifier.get('classifier_id') == finding.classifier_id:
                        classifier_config = classifier
                        break
                
                if not classifier_config:
                    print(f"Warning: No config found for classifier '{finding.classifier_id}'")
                    continue
                
                # 3. Apply dictionary boosts
                confidence_components = self._apply_dictionary_boosts(
                    finding,
                    row_idx,
                    field_name,
                    classifier_config,
                    table_data,
                    chunk_text
                )
                
                # 4. NEW: Add validation boost if finding passed validation
                # Note: If we got this finding, it already passed validation in engine.py
                validation_rules = classifier_config.get('validation_rules', [])
                if validation_rules:
                    # Finding passed validation (otherwise it would have been filtered)
                    config = self.config.confidence_scoring if hasattr(self, 'config') else None
                    validation_boost = config.validation_passed_boost if config else 0.3
                    confidence_components.validation_boost = validation_boost
                
                # 5. NEW: Get match and column statistics
                if self.strategy == ProcessingStrategy.COLUMNAR:
                    cache_key = f"{finding.classifier_id}:{field_name}"
                else:
                    cache_key = f"{finding.classifier_id}:multiple"
                
                match_stats = self.match_stats_cache.get(cache_key)
                column_stats = self.column_stats_cache.get(field_name) if is_structured else None
                
                # 6. NEW: Calculate weighted confidence
                if match_stats:
                    final_confidence, confidence_tier = self._calculate_weighted_confidence(
                        confidence_components,
                        match_stats,
                        column_stats,
                        is_structured
                    )
                else:
                    # Fallback if no stats available
                    confidence_components.calculate_final()
                    final_confidence = confidence_components.final_confidence
                    confidence_tier = self._score_to_tier(final_confidence, 
                        self.config.confidence_scoring if hasattr(self, 'config') else None)
                
                # 7. Update finding with all data
                finding.confidence_score = final_confidence
                
                # Enrich context_data
                if not hasattr(finding, 'context_data') or finding.context_data is None:
                    finding.context_data = {}
                
                finding.context_data.update({
                    'row_idx': row_idx,
                    'field_name': field_name,
                    'chunk_id': chunk_id,
                    'confidence_tier': confidence_tier,  # NEW
                    'confidence_components': {
                        'presidio_base_score': confidence_components.presidio_base_score,
                        'column_name_boost': confidence_components.column_name_boost,
                        'words_boost': confidence_components.words_boost,
                        'exact_match_boost': confidence_components.exact_match_boost,
                        'negative_penalty': confidence_components.negative_penalty,
                        'cross_column_boost': confidence_components.cross_column_boost,
                        'validation_boost': confidence_components.validation_boost,  # NEW
                        'final_confidence': final_confidence
                    }
                })
                
                # 8. Update match statistics
                if match_stats:
                    applied_boosts = {
                        'column_name_boost': confidence_components.column_name_boost,
                        'words_boost': confidence_components.words_boost,
                        'exact_match_boost': confidence_components.exact_match_boost,
                        'negative_penalty': confidence_components.negative_penalty,
                        'cross_column_boost': confidence_components.cross_column_boost,
                        'validation_boost': confidence_components.validation_boost  # NEW
                    }
                    match_stats.update_with_finding(finding, applied_boosts)
                
                # 9. Add to results
                processed_findings.append(finding)
            
            return processed_findings
            
        except Exception as e:
            print(f"Error: Failed to process chunk '{chunk_id}': {e}")
            return []


    async def _process_columnar(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any],
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Process table column-by-column with chunking (Strategy #5)
        
        Flow:
        1. For each column:
           a. Extract all values (up to MAX_ROWS_PER_COLUMN)
           b. Build chunks with position maps
           c. Process each chunk
           d. Collect findings
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            classifier_engine: ClassificationEngine instance
            active_classifiers: List of classifier configurations
            
        Returns:
            List of all PIIFinding objects from all columns
        """
        try:
            all_findings = []
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            # Limit rows if table is too large
            rows_to_process = table_data[:MAX_ROWS_PER_COLUMN]
            
            print(f"Info: Processing {len(filtered_fields)} columns with columnar strategy")
            
            for field_name in filtered_fields:
                # Extract all values for this column
                column_values = [row.get(field_name) for row in rows_to_process]
                
                # Build chunks with position maps
                chunks = self.field_mapper.build_column_chunks_with_mapping(
                    column_values=column_values,
                    field_name=field_name,
                    start_row_idx=0,
                    max_chunk_size=MAX_CHUNK_SIZE
                )
                
                if not chunks:
                    continue
                
                print(f"Info: Column '{field_name}' split into {len(chunks)} chunks")
                
                # Process each chunk
                for chunk_text, position_map, chunk_id in chunks:
                    # Prepare context info
                    context_info = {
                        'object_type': 'database_column',
                        'field_name': field_name,
                        'table_name': table_metadata.get('table_name'),
                        'schema_name': table_metadata.get('schema_name'),
                        'database_name': table_metadata.get('database_name'),
                        'processing_strategy': 'columnar',
                        'table_metadata': table_metadata
                    }
                    
                    # Process chunk
                    chunk_findings = await self._process_chunk(
                        chunk_text=chunk_text,
                        position_map=position_map,
                        chunk_id=chunk_id,
                        context_info=context_info,
                        table_data=rows_to_process,
                        classifier_engine=classifier_engine,
                        active_classifiers=active_classifiers
                    )
                    
                    all_findings.extend(chunk_findings)
            
            print(f"Info: Columnar processing completed - {len(all_findings)} total findings")
            
            return all_findings
            
        except Exception as e:
            print(f"Error: Failed to process table with columnar strategy: {e}")
            # Return empty list on error
            return []


    async def _process_row_based_with_names(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any],
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Process table row-by-row with field names (Strategy #2)
        
        Flow:
        1. For each row:
           a. Build row text with field names
           b. Process through engine
           c. Map findings and apply boosts
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            classifier_engine: ClassificationEngine instance
            active_classifiers: List of classifier configurations
            
        Returns:
            List of all PIIFinding objects from all rows
        """
        try:
            all_findings = []
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            # Limit rows if table is too large
            rows_to_process = table_data[:MAX_ROWS_PER_COLUMN]
            
            print(f"Info: Processing {len(rows_to_process)} rows with row-based (with names) strategy")
            
            for row_idx, row_data in enumerate(rows_to_process):
                # Build row text with field names
                row_text, position_map = self.field_mapper.build_row_text_with_mapping(
                    row_data=row_data,
                    row_idx=row_idx,
                    filtered_fields=filtered_fields
                )
                
                # Skip empty rows
                if not row_text.strip():
                    continue
                
                # Prepare context info
                context_info = {
                    'object_type': 'database_row',
                    'table_name': table_metadata.get('table_name'),
                    'schema_name': table_metadata.get('schema_name'),
                    'database_name': table_metadata.get('database_name'),
                    'processing_strategy': 'row_based_with_names',
                    'row_data': row_data,
                    'table_metadata': table_metadata
                }
                
                chunk_id = f"row_{row_idx}"
                
                # Process row
                row_findings = await self._process_chunk(
                    chunk_text=row_text,
                    position_map=position_map,
                    chunk_id=chunk_id,
                    context_info=context_info,
                    table_data=rows_to_process,
                    classifier_engine=classifier_engine,
                    active_classifiers=active_classifiers
                )
                
                all_findings.extend(row_findings)
            
            print(f"Info: Row-based (with names) processing completed - {len(all_findings)} total findings")
            
            return all_findings
            
        except Exception as e:
            print(f"Error: Failed to process table with row-based (with names) strategy: {e}")
            # Return empty list on error
            return []


    async def _process_row_based_no_names(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any],
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Process table row-by-row WITHOUT field names (Strategy #1)
        
        Flow:
        1. For each row:
           a. Build row text without field names
           b. Process through engine
           c. Map findings and apply boosts
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            classifier_engine: ClassificationEngine instance
            active_classifiers: List of classifier configurations
            
        Returns:
            List of all PIIFinding objects from all rows
        """
        try:
            all_findings = []
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            # Limit rows if table is too large
            rows_to_process = table_data[:MAX_ROWS_PER_COLUMN]
            
            print(f"Info: Processing {len(rows_to_process)} rows with row-based (no names) strategy")
            
            for row_idx, row_data in enumerate(rows_to_process):
                # Build row text without field names
                row_text, position_map = self.field_mapper.build_row_text_no_names(
                    row_data=row_data,
                    row_idx=row_idx,
                    filtered_fields=filtered_fields
                )
                
                # Skip empty rows
                if not row_text.strip():
                    continue
                
                # Prepare context info
                context_info = {
                    'object_type': 'database_row',
                    'table_name': table_metadata.get('table_name'),
                    'schema_name': table_metadata.get('schema_name'),
                    'database_name': table_metadata.get('database_name'),
                    'processing_strategy': 'row_based_no_names',
                    'row_data': row_data,
                    'table_metadata': table_metadata
                }
                
                chunk_id = f"row_{row_idx}"
                
                # Process row
                row_findings = await self._process_chunk(
                    chunk_text=row_text,
                    position_map=position_map,
                    chunk_id=chunk_id,
                    context_info=context_info,
                    table_data=rows_to_process,
                    classifier_engine=classifier_engine,
                    active_classifiers=active_classifiers
                )
                
                all_findings.extend(row_findings)
            
            print(f"Info: Row-based (no names) processing completed - {len(all_findings)} total findings")
            
            return all_findings
            
        except Exception as e:
            print(f"Error: Failed to process table with row-based (no names) strategy: {e}")
            # Return empty list on error
            return []


    async def _process_multirow_with_names(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any],
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Process table with multiple rows per chunk WITH field names (Strategy #3)
        
        Flow:
        1. Build chunks containing multiple rows
        2. Process each chunk
        3. Collect all findings
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            classifier_engine: ClassificationEngine instance
            active_classifiers: List of classifier configurations
            
        Returns:
            List of all PIIFinding objects from all rows
        """
        try:
            all_findings = []
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            # Limit rows if table is too large
            rows_to_process = table_data[:MAX_ROWS_PER_COLUMN]
            
            print(f"Info: Processing {len(rows_to_process)} rows with multi-row (with names) strategy")
            
            # Build chunks with multiple rows
            chunks = self.field_mapper.build_multirow_text_with_names(
                rows_data=rows_to_process,
                start_row_idx=0,
                filtered_fields=filtered_fields,
                max_chunk_size=MAX_CHUNK_SIZE
            )
            
            if not chunks:
                print(f"Warning: No chunks generated for multi-row (with names) strategy")
                return []
            
            print(f"Info: Generated {len(chunks)} multi-row chunks")
            
            # Process each chunk
            for chunk_text, position_map, chunk_id in chunks:
                # Prepare context info
                context_info = {
                    'object_type': 'database_multirow',
                    'table_name': table_metadata.get('table_name'),
                    'schema_name': table_metadata.get('schema_name'),
                    'database_name': table_metadata.get('database_name'),
                    'processing_strategy': 'multirow_with_names',
                    'table_metadata': table_metadata
                }
                
                # Process chunk
                chunk_findings = await self._process_chunk(
                    chunk_text=chunk_text,
                    position_map=position_map,
                    chunk_id=chunk_id,
                    context_info=context_info,
                    table_data=rows_to_process,
                    classifier_engine=classifier_engine,
                    active_classifiers=active_classifiers
                )
                
                all_findings.extend(chunk_findings)
            
            print(f"Info: Multi-row (with names) processing completed - {len(all_findings)} total findings")
            
            return all_findings
            
        except Exception as e:
            print(f"Error: Failed to process table with multi-row (with names) strategy: {e}")
            return []


    async def _process_multirow_no_names(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any],
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Process table with multiple rows per chunk WITHOUT field names (Strategy #4)
        
        Flow:
        1. Build chunks containing multiple rows
        2. Process each chunk
        3. Collect all findings
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            classifier_engine: ClassificationEngine instance
            active_classifiers: List of classifier configurations
            
        Returns:
            List of all PIIFinding objects from all rows
        """
        try:
            all_findings = []
            filtered_fields = filter_fields_for_classification(table_metadata)
            
            # Limit rows if table is too large
            rows_to_process = table_data[:MAX_ROWS_PER_COLUMN]
            
            print(f"Info: Processing {len(rows_to_process)} rows with multi-row (no names) strategy")
            
            # Build chunks with multiple rows
            chunks = self.field_mapper.build_multirow_text_no_names(
                rows_data=rows_to_process,
                start_row_idx=0,
                filtered_fields=filtered_fields,
                max_chunk_size=MAX_CHUNK_SIZE
            )
            
            if not chunks:
                print(f"Warning: No chunks generated for multi-row (no names) strategy")
                return []
            
            print(f"Info: Generated {len(chunks)} multi-row chunks")
            
            # Process each chunk
            for chunk_text, position_map, chunk_id in chunks:
                # Prepare context info
                context_info = {
                    'object_type': 'database_multirow',
                    'table_name': table_metadata.get('table_name'),
                    'schema_name': table_metadata.get('schema_name'),
                    'database_name': table_metadata.get('database_name'),
                    'processing_strategy': 'multirow_no_names',
                    'table_metadata': table_metadata
                }
                
                # Process chunk
                chunk_findings = await self._process_chunk(
                    chunk_text=chunk_text,
                    position_map=position_map,
                    chunk_id=chunk_id,
                    context_info=context_info,
                    table_data=rows_to_process,
                    classifier_engine=classifier_engine,
                    active_classifiers=active_classifiers
                )
                
                all_findings.extend(chunk_findings)
            
            print(f"Info: Multi-row (no names) processing completed - {len(all_findings)} total findings")
            
            return all_findings
            
        except Exception as e:
            print(f"Error: Failed to process table with multi-row (no names) strategy: {e}")
            return []

    async def process_table(
        self,
        table_data: List[Dict[str, Any]],
        table_metadata: Dict[str, Any],
        strategy: ProcessingStrategy,
        classifier_engine,
        active_classifiers: List[Dict[str, Any]]
    ) -> List[PIIFinding]:
        """
        Main entry point - processes table with specified strategy
        
        Flow:
        1. Store strategy
        2. Calculate column statistics (always)
        3. Initialize match statistics
        4. Dispatch to strategy handler
        5. Apply deferred CATEGORY_CO_OCCURRENCE boosts
        6. Finalize match statistics
        7. Return complete findings
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            strategy: ProcessingStrategy to use
            classifier_engine: ClassificationEngine instance
            active_classifiers: List of classifier configurations
            
        Returns:
            List of complete PIIFinding objects with all boosts applied
        """
        try:
            # Store strategy for use by other methods
            self.strategy = strategy
            
            print(f"Info: Starting table processing with strategy: {strategy.value}")
            
            # 1. Calculate column statistics (always done regardless of strategy)
            self._calculate_all_column_statistics(table_data, table_metadata)
            
            # 2. Initialize match statistics for all classifiers
            self._initialize_match_statistics(active_classifiers, table_metadata)
            
            # 3. Dispatch to appropriate strategy handler
            if strategy == ProcessingStrategy.COLUMNAR:
                findings = await self._process_columnar(
                    table_data, table_metadata, classifier_engine, active_classifiers
                )
                
            elif strategy == ProcessingStrategy.ROW_BASED_WITH_NAMES:
                findings = await self._process_row_based_with_names(
                    table_data, table_metadata, classifier_engine, active_classifiers
                )
                
            elif strategy == ProcessingStrategy.ROW_BASED_NO_NAMES:
                findings = await self._process_row_based_no_names(
                    table_data, table_metadata, classifier_engine, active_classifiers
                )
                
            elif strategy == ProcessingStrategy.MULTI_ROW_WITH_NAMES:
                findings = await self._process_multirow_with_names(
                    table_data, table_metadata, classifier_engine, active_classifiers
                )
                
            elif strategy == ProcessingStrategy.MULTI_ROW_NO_NAMES:
                findings = await self._process_multirow_no_names(
                    table_data, table_metadata, classifier_engine, active_classifiers
                )
            
            else:
                raise ProcessingError(
                    f"Unsupported processing strategy: {strategy}",
                    ErrorType.SYSTEM_INTERNAL_ERROR
                )
            
            # 4. Apply deferred CATEGORY_CO_OCCURRENCE boosts
            findings = self._apply_category_cooccurrence_boosts(
                findings, table_data, active_classifiers
            )
            
            # 5. Finalize match statistics
            self._finalize_match_statistics(len(table_data))
            
            # 6. Attach column and match statistics to findings
            for finding in findings:
                field_name = finding.context_data.get('field_name')
                
                # Attach column statistics
                if field_name and field_name in self.column_stats_cache:
                    finding.context_data['column_statistics'] = self.column_stats_cache[field_name]
                
                # Attach match statistics
                if strategy == ProcessingStrategy.COLUMNAR:
                    cache_key = f"{finding.classifier_id}:{field_name}"
                else:
                    cache_key = f"{finding.classifier_id}:multiple"
                
                if cache_key in self.match_stats_cache:
                    finding.context_data['match_statistics'] = self.match_stats_cache[cache_key]
            
            print(f"Info: Table processing completed - {len(findings)} total findings")
            
            return findings
            
        except ProcessingError:
            # Re-raise ProcessingError as-is
            raise
        except Exception as e:
            print(f"Error: Failed to process table: {e}")
            raise ProcessingError(
                f"Table processing failed: {str(e)}",
                ErrorType.SYSTEM_INTERNAL_ERROR
            ) from e


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
            # --- ADD THIS LOGGING ---
            # This will print the exact string sent to the classification engine for each row.
            #print("-" * 25, " ROW PROCESSOR DEBUG ", "-" * 25)
            #print(f"Row PK: {row_pk}")
            #print(f"Combined Text Sent to Engine: '{combined_text}'")
            #print("-" * 75)
            # --- END OF LOGGING ---            
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
            # --- THIS IS THE FIX ---
            # Run the synchronous, CPU-bound classification in a separate thread
            # to prevent blocking the worker's async event loop.
            loop = asyncio.get_running_loop()
            findings = await loop.run_in_executor(
                None,  # Use the default thread pool executor
                classifier.classify_content,  # The synchronous function to run
                combined_text,  # The first argument to the function
                context_info    # The second argument to the function
            )
            # --- END OF FIX ---
            
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

            
            #return structured_findings
            # --- START OF CHANGE ---
            # The method now enriches and returns the original PIIFinding objects
            # instead of converting them to a different dictionary structure.
            all_findings = []
            for field_name, findings_list in field_findings.items():
                for finding in findings_list:
                    if not hasattr(finding, 'context_data') or finding.context_data is None:
                        finding.context_data = {}
                    finding.context_data.update({
                        "field_name": field_name,
                        **context_info
                    })
                    all_findings.append(finding)
            
            return all_findings
            # --- END OF CHANGE ---            
        except Exception as e:
            # Return empty results on error, log for debugging
            raise ProcessingError(f"Error processing row {row_pk}: {str(e)}") from e
            
    
    async def process_document_content_nottouse(self,
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
            # --- FIX IS HERE ---
            # Run the synchronous, CPU-bound classification in a separate thread
            # to prevent blocking the worker's async event loop.
            loop = asyncio.get_running_loop()
            findings = await loop.run_in_executor(
                None,  # Use the default thread pool executor
                classifier.classify_content,  # The synchronous function to run
                content,       # The first argument
                context_info   # The second argument
            )
            
            # Enrich findings with context data before returning
            for finding in findings:
                if not hasattr(finding, 'context_data') or finding.context_data is None:
                    finding.context_data = {}
                finding.context_data.update(context_info)
            
            return findings

            
        except Exception as e:
            print(f"Error processing document {file_metadata.get('file_path')}: {str(e)}")
            return []

    async def process_document_content(self,
                                     content: str,
                                     file_metadata: Dict[str, Any],
                                     classifier) -> List[Dict[str, Any]]:
        """
        Process document content for PII detection with chunking for large files
        """
        try:
            # Presidio recommended maximum: 50K-100K chars per analysis
            MAX_CHUNK_SIZE = 50000
            
            # Prepare context for classification
            context_info = {
                "object_type": "document",
                "file_path": file_metadata.get('file_path'),
                "file_name": file_metadata.get('file_name'),
                "file_size": file_metadata.get('file_size', 0),
                "file_extension": file_metadata.get('file_extension'),
                "content_extracted_size": len(content)
            }
            
            all_findings = []
            
            # If content is small enough, process in one go
            if len(content) <= MAX_CHUNK_SIZE:
                # --- FIX IS HERE ---
                # Run the synchronous, CPU-bound classification in a separate thread
                # to prevent blocking the worker's async event loop.
                loop = asyncio.get_running_loop()
                findings = await loop.run_in_executor(
                    None,  # Use the default thread pool executor
                    classifier.classify_content,  # The synchronous function to run
                    content,       # The first argument
                    context_info   # The second argument
                )
                all_findings.extend(findings)
            else:
                # Chunk large content
                offset = 0
                chunk_num = 0
                
                while offset < len(content):
                    chunk = content[offset:offset + MAX_CHUNK_SIZE]
                    chunk_num += 1
                    
                    chunk_context = {
                        **context_info,
                        "chunk_number": chunk_num,
                        "chunk_offset": offset,
                        "is_chunked": True
                    }
                    
                    loop = asyncio.get_running_loop()
                    chunk_findings = await loop.run_in_executor(
                        None,
                        classifier.classify_content,
                        chunk,
                        chunk_context
                    )
                    
                    # Adjust positions for chunk offset
                    for finding in chunk_findings:
                        finding.start_position += offset
                        finding.end_position += offset
                    
                    all_findings.extend(chunk_findings)
                    offset += MAX_CHUNK_SIZE
            
            # Enrich findings with context data before returning
            for finding in all_findings:
                if not hasattr(finding, 'context_data') or finding.context_data is None:
                    finding.context_data = {}
                finding.context_data.update(context_info)
            
            return all_findings
        
        except Exception as e:
            print(f"Error processing document {file_metadata.get('file_path')}: {str(e)}")
            return []

    def _assess_data_quality(self, column_stats: ColumnStatistics) -> float:
        """
        Assess data quality from column statistics.
        
        Returns a score between 0.0 and 1.0 based on:
        - Low null percentage (good)
        - High uniqueness (good for PII)
        - Consistent length (good)
        
        Args:
            column_stats: Column statistics object
            
        Returns:
            Quality score 0.0 (poor) to 1.0 (excellent)
        """
        quality_score = 0.0
        
        # Factor 1: Null percentage (lower is better)
        # 0% nulls = 0.4, 50% nulls = 0.2, 80%+ nulls = 0.0
        null_factor = max(0.0, (1.0 - column_stats.null_percentage / 100) * 0.4)
        quality_score += null_factor
        
        # Factor 2: Distinctness (higher is better for PII)
        # 100% distinct = 0.4, 50% distinct = 0.2, <1% distinct = 0.0
        distinct_factor = min(0.4, column_stats.distinct_value_percentage / 100 * 0.4)
        quality_score += distinct_factor
        
        # Factor 3: Length consistency (coefficient of variation)
        # If min/max/mean are available, check consistency
        if column_stats.max_value_length > 0:
            length_range = column_stats.max_value_length - column_stats.min_value_length
            avg_length = column_stats.mean_value_length
            
            if avg_length > 0:
                # Coefficient of variation (lower is more consistent)
                cv = length_range / avg_length
                # cv < 0.1 = very consistent (0.2), cv > 1.0 = inconsistent (0.0)
                consistency_factor = max(0.0, min(0.2, 0.2 * (1.0 - cv)))
                quality_score += consistency_factor
            else:
                # No average length, assume neutral
                quality_score += 0.1
        else:
            # No length data, assume neutral
            quality_score += 0.1
        
        return min(1.0, quality_score)


    def _apply_quick_filters(
        self,
        match_stats: MatchStatistics,
        column_stats: Optional[ColumnStatistics]
    ) -> bool:
        """
        Apply quick filters to detect automatic LOW confidence cases.
        
        Returns:
            True if finding should be marked LOW (failed filters)
            False if finding passes filters (continue to formula)
        """
        config = self.config.confidence_scoring if hasattr(self, 'config') else None
        
        if not config:
            # No config available, skip filters
            return False
        
        # Filter 1: Low regex match rate
        if match_stats.regex_match_rate < config.min_regex_match_rate:
            return True
        
        # Filter 2: High null percentage (only for structured data)
        if column_stats and column_stats.null_percentage > config.max_null_percentage:
            return True
        
        # Filter 3: Very low distinctness (test data pattern)
        if column_stats and column_stats.distinct_value_percentage < config.min_distinct_percentage:
            return True
        
        return False


    def _calculate_weighted_confidence(
        self,
        components: ConfidenceComponents,
        match_stats: MatchStatistics,
        column_stats: Optional[ColumnStatistics],
        is_structured: bool
    ) -> Tuple[float, str]:
        """
        Calculate final confidence score using weighted formula.
        
        Applies different formulas for structured (database) vs unstructured (file) data.
        
        Args:
            components: All confidence components (dictionary boosts, validation, etc.)
            match_stats: Match statistics for this classifier
            column_stats: Column statistics (None for file data)
            is_structured: True for database, False for file
            
        Returns:
            Tuple of (confidence_score, confidence_tier)
            - confidence_score: Float between 0.0 and 1.0
            - confidence_tier: "HIGH", "MEDIUM", or "LOW"
        """
        # Get configuration
        config = self.config.confidence_scoring if hasattr(self, 'config') else None
        
        if not config:
            # Fallback to simple sum if no config
            components.calculate_final()
            tier = self._score_to_tier(components.final_confidence, config)
            return components.final_confidence, tier  # ✅ FIXED: Return tuple
        
        # Check quick filters first (automatic LOW)
        if self._apply_quick_filters(match_stats, column_stats):
            return 0.0, "LOW"
        
        confidence = 0.0
        
        if is_structured and column_stats:
            # ============================================================
            # DATABASE/STRUCTURED FORMULA
            # ============================================================
            
            # Factor 1: Regex match rate (weight: 0.3)
            match_rate_contribution = (match_stats.regex_match_rate / 100) * config.database_match_rate_weight
            confidence += match_rate_contribution
            
            # Factor 2: Data quality assessment (weight: 0.2)
            quality_score = self._assess_data_quality(column_stats)
            quality_contribution = quality_score * config.database_data_quality_weight
            confidence += quality_contribution
            
            # Factor 3: Column name match (weight: 0.3)
            if components.column_name_boost > 0:
                confidence += config.database_column_name_weight
            
            # Factor 4: Words context (weight: 0.1)
            if components.words_boost > 0:
                confidence += config.database_words_weight
            
            # Factor 5: Exact match (weight: 0.1)
            if components.exact_match_boost > 0:
                confidence += config.database_exact_match_weight
            
            # Factor 6: Validation boost (additive)
            confidence += components.validation_boost
            
            # Factor 7: Cross-column boost (additive)
            confidence += components.cross_column_boost
            
            # Factor 8: Negative penalty
            confidence += components.negative_penalty
            
        else:
            # ============================================================
            # FILE/UNSTRUCTURED FORMULA
            # ============================================================
            
            # Factor 1: Words context (weight: 0.4)
            if components.words_boost > 0:
                confidence += config.file_words_weight
            
            # Factor 2: Exact match (weight: 0.4)
            if components.exact_match_boost > 0:
                confidence += config.file_exact_match_weight
            
            # Factor 3: Pattern match (weight: 0.2)
            confidence += components.presidio_base_score * config.file_pattern_weight
            
            # Factor 4: Validation boost (additive)
            confidence += components.validation_boost
            
            # Factor 5: Cross-column boost (additive)
            confidence += components.cross_column_boost
            
            # Factor 6: Negative penalty
            confidence += components.negative_penalty
        
        # Clamp to [0.0, 1.0]
        confidence = max(0.0, min(1.0, confidence))
        
        # Convert to tier
        tier = self._score_to_tier(confidence, config)
        
        return confidence, tier  


    def _score_to_tier(self, score: float, config) -> str:
        """
        Convert confidence score to categorical tier.
        
        Args:
            score: Confidence score between 0.0 and 1.0
            config: Configuration with thresholds
            
        Returns:
            "HIGH", "MEDIUM", or "LOW"
        """
        if not config:
            # Default thresholds
            if score >= 0.7:
                return "HIGH"
            elif score >= 0.4:
                return "MEDIUM"
            else:
                return "LOW"
        
        if score >= config.high_confidence_min_score:
            return "HIGH"
        elif score >= config.medium_confidence_min_score:
            return "MEDIUM"
        else:
            return "LOW"

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