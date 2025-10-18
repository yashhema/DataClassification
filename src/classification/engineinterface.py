"""
The primary, unified interface for the entire classification subsystem.

This class encapsulates all the internal logic for configuration loading,
engine instantiation, data processing, and results formatting, providing
a clean, simple API to the rest of the application (e.g., the Orchestrator).
"""

import asyncio
import json
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict

from core.errors import ProcessingError
from .engine import ClassificationEngine
from .row_processor import RowProcessor
from core.db.database_interface import DatabaseInterface
from core.config.configuration_manager import ConfigurationManager, ClassificationConfidenceConfig
from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler, ClassificationError, ErrorType
from core.models.models import PIIFinding, ContentComponent  
from core.utils.hash_utils import generate_finding_key_hash
from classification.generic_enums import ProcessingStrategy  
# =============================================================================
# Helper Class: Configuration Loader (Internal to the Interface)
# =============================================================================

class _ConfigurationLoader:
    """
    An internal helper class to translate database ORM objects into the
    dictionaries required by the ClassificationEngine.
    """
    def __init__(self, db_interface: DatabaseInterface, job_context: Dict[str, Any]):
        self.db_interface = db_interface
        self.job_context = job_context

    async def load_and_assemble(self, template_id: str, system_logger: SystemLogger, error_handler: ErrorHandler) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        orm_template = await self.db_interface.get_classifier_template_full(template_id, context=self.job_context)
        if not orm_template:
            raise ClassificationError(f"Template '{template_id}' not found.", ErrorType.CONFIGURATION_MISSING)

        nested_config = orm_template.configuration or '{}'
        template_config = {
            "template_id": orm_template.template_id,
            "name": orm_template.name,
            "classifiers": [{"classifier_id": c.classifier_id} for c in orm_template.classifiers],
            **nested_config
        }

        classifier_configs = {}
        for orm_classifier in orm_template.classifiers:
            # Build base classifier config
            config = {
                "classifier_id": orm_classifier.classifier_id,
                "entity_type": orm_classifier.entity_type,
                "name": orm_classifier.name,
                "requires_row_context": orm_classifier.requires_row_context,  # NEW: Added boolean flag
                "patterns": [{"name": p.name, "regex": p.regex, "score": p.score} for p in orm_classifier.patterns],
                "context_rules": [{"rule_type": p.rule_type.value, "regex": p.regex, "window_before": p.window_before, "window_after": p.window_after} for p in orm_classifier.context_rules],
                "validation_rules": [{"validation_fn_name": v.validation_fn_name} for v in orm_classifier.validation_rules],
                "exclude_list": [e.term_to_exclude for e in getattr(orm_classifier, 'exclude_list', [])]
            }
            
            # NEW: Add dictionary if present (optional field)
            if orm_classifier.dictionary is not None:
                dict_orm = orm_classifier.dictionary
                config["dictionary"] = {
                    "column_names": dict_orm.column_names,      # Already a dict from JSON column
                    "words": dict_orm.words,                    # Already a dict from JSON column
                    "exact_match": dict_orm.exact_match,        # Already a dict from JSON column
                    "negative": dict_orm.negative,              # Already a dict from JSON column
                    "cross_column_support": dict_orm.cross_column_support  # Already a dict from JSON column
                }
            
            classifier_configs[orm_classifier.classifier_id] = config
        
        system_logger.debug(f"Assembled template configuration: {json.dumps(template_config, indent=2)}")
        system_logger.debug(f"Assembled classifier configurations: {json.dumps(classifier_configs, indent=2)}")
            
        return template_config, classifier_configs
# =============================================================================
# The Main Engine Interface
# =============================================================================

class EngineInterface:
    """
    The single, public-facing service layer for all classification tasks.
    """
    def __init__(self, db_interface: DatabaseInterface, config_manager: ConfigurationManager, system_logger: SystemLogger, error_handler: ErrorHandler, job_context: Dict[str, Any]):
        self.db_interface = db_interface
        self.config = config_manager.settings.classification
        self.logger = system_logger
        self.error_handler = error_handler
        self.job_context = job_context

        self._engine: Optional[ClassificationEngine] = None
        self._row_processor: Optional[RowProcessor] = None

    async def initialize_for_template(self, template_id: str):
        """
        Initializes the internal engine and processor for a specific template.
        Must be called before any classification methods.
        """
        try:
            self.logger.info(f"Initializing EngineInterface for template '{template_id}'", **self.job_context)
            loader = _ConfigurationLoader(self.db_interface, self.job_context)
            template_config, classifier_configs =await loader.load_and_assemble(template_id,self.logger,self.error_handler)
            
            self._engine = ClassificationEngine(template_config, classifier_configs, self.error_handler)
            self._row_processor = RowProcessor()
            self.logger.info(f"EngineInterface initialized successfully for template '{template_id}'.", **self.job_context)
        except Exception as e:
            # Let the handler convert and log the error, then re-raise
            raise self.error_handler.handle_error(e, "engine_interface_initialization", **self.job_context)

    async def classify_database_row(self, row_data: Dict[str, Any], row_pk: Dict[str, Any], table_metadata: Dict[str, Any]) -> List[PIIFinding]:
        """
        Classifies a single row of structured data.

        Returns:
            A list of PIIFinding objects for the row.
        """
        if not self._engine or not self._row_processor:
            raise ProcessingError("EngineInterface not initialized.", ErrorType.SYSTEM_INTERNAL_ERROR)
        
        try:
            # The RowProcessor internally calls the engine's classify_content method
            return await self._row_processor.process_database_row(row_data, row_pk, table_metadata, self._engine)
        except Exception as e:
            context = {
                **self.job_context,
                "table_name": table_metadata.get("table_name"),
                "schema_name": table_metadata.get("schema_name")
                }
            # --- ADD THIS LOGGING ---
            self.logger.error(
                "An unhandled exception occurred during document content classification in datarow.",
                exc_info=True,  # This is the key part that adds the traceback
                **context
            )
            # Do not re-raise; just log the error and return empty list to allow the job to continue
            self.error_handler.handle_error(e, "classify_database_row", **context)
            return []

    async def classify_document_content(self, content: str, file_metadata: Dict[str, Any]) -> List[PIIFinding]:
        """
        Classifies a blob of unstructured text content.

        Returns:
            A list of PIIFinding objects for the document.
        """
        if not self._engine or not self._row_processor:
            raise ProcessingError("EngineInterface not initialized.", ErrorType.SYSTEM_INTERNAL_ERROR)
        
        try:
            return await self._row_processor.process_document_content(content, file_metadata, self._engine)
        except Exception as e:
            context = {**self.job_context, "file_path": file_metadata.get("file_path")}

            # --- ADD THIS LOGGING ---
            self.logger.error(
                "An unhandled exception occurred during document content classification in datarow.",
                exc_info=True,  # This is the key part that adds the traceback
                **context
            )
            self.error_handler.handle_error(e, "classify_document_content", **context)
            return []

    def convert_findings_to_db_format(self, all_findings: List[PIIFinding], total_rows_scanned: int) -> List[Dict[str, Any]]:
        """
        Converts findings with improved tier-aware aggregation.
        """
        import os  # ✅ ADD: Need for file path operations
        
        safe_job_context = {k: str(v) if hasattr(v, 'isoformat') else v for k, v in self.job_context.items()}
        self.logger.info(f"DEBUG: job_context in convert_findings_to_db_format: {safe_job_context}")
        
        if not all_findings:
            return []
        
        # Determine source type
        first_finding_context = all_findings[0].context_data or {}
        is_file_source = 'file_path' in first_finding_context

        # ======================================================================
        # FILE AGGREGATION LOGIC - FIXED
        # ======================================================================
        if is_file_source:
            # ✅ FIXED: Group by (classifier_id, file_path) instead of just classifier_id
            summary_map = defaultdict(list)
            for finding in all_findings:
                context = finding.context_data or {}
                file_path = context.get('file_path', '') or ''  # Handle None
                key = (finding.classifier_id, file_path)
                summary_map[key].append(finding)
            
            db_records = []
            for (classifier_id, file_path), findings_list in summary_map.items():
                first_finding = findings_list[0]
                context = first_finding.context_data or {}
                scores = [f.confidence_score for f in findings_list]
                
                # ✅ FIXED: Tier aggregation using percentage-based logic
                tiers = [f.context_data.get('confidence_tier', 'LOW') if f.context_data else 'LOW' 
                         for f in findings_list]
                
                # Count tier distribution
                high_count = tiers.count('HIGH')
                medium_count = tiers.count('MEDIUM')
                low_count = tiers.count('LOW')
                total = len(tiers)
                
                # Calculate percentages
                if total > 0:
                    high_percent = (high_count / total * 100)
                    medium_percent = (medium_count / total * 100)
                    combined_high_medium_percent = ((high_count + medium_count) / total * 100)
                    
                    # File-level tier logic (more lenient than columns)
                    if high_percent >= 60:
                        aggregated_tier = "HIGH"
                    elif combined_high_medium_percent >= 70:
                        aggregated_tier = "HIGH"
                    elif high_percent >= 20 or combined_high_medium_percent >= 40:
                        aggregated_tier = "MEDIUM"
                    else:
                        aggregated_tier = "LOW"
                else:
                    aggregated_tier = "LOW"
                
                # ✅ FIXED: Calculate distinct matches for file-level stats
                unique_texts = set()
                for f in findings_list:
                    unique_texts.add(f.text)
                
                distinct_match_count = len(unique_texts)
                total_match_count = len(findings_list)
                
                samples = [{"text": f.text, "confidence": f.confidence_score} 
                          for f in findings_list[:self.config.max_samples_per_finding]]

                # Match statistics from first finding (template for structure)
                match_stats = context.get('match_statistics')
                
                # ✅ FIXED: Extract file metadata properly
                if file_path:
                    file_name = os.path.basename(file_path)
                    file_extension = os.path.splitext(file_name)[1] if file_name else None
                else:
                    file_name = None
                    file_extension = None
                
                finding_context = {
                    "data_source_id": self.job_context.get("datasource_id"),
                    "classifier_id": classifier_id,
                    "entity_type": first_finding.entity_type,
                    "schema_name": context.get("table_metadata", {}).get("schema_name"),
                    "table_name": None,
                    "field_name": None,
                    "file_path": file_path,
                    "file_name": file_name
                } 
                finding_key_hash = generate_finding_key_hash(finding_context)
                
                record = {
                    'finding_key_hash': finding_key_hash,
                    "scan_job_id": self.job_context.get("job_id"),
                    "data_source_id": self.job_context.get("datasource_id"),
                    "classifier_id": classifier_id,
                    "entity_type": first_finding.entity_type,
                    "schema_name": None,
                    "table_name": None,
                    "field_name": None,
                    "file_path": file_path,  # ✅ Full path
                    "file_name": file_name,  # ✅ Basename only
                    "file_extension": file_extension,  # ✅ Extracted
                    "finding_count": len(findings_list),
                    "average_confidence": sum(scores) / len(scores) if scores else 0,
                    "max_confidence": max(scores) if scores else 0,
                    "confidence_tier": aggregated_tier,
                    "sample_findings": json.dumps(samples),
                    "total_rows_in_source": total_rows_scanned,
                    
                    # Column Statistics (NULL for files)
                    "null_percentage": None,
                    "min_length": None,
                    "max_length": None,
                    "mean_length": None,
                    "distinct_value_count": None,
                    "distinct_value_percentage": None,
                    
                    # ✅ FIXED: Match Statistics (calculated for file)
                    "total_regex_matches": total_match_count,  # Total findings
                    "regex_match_rate": 0.0,  # N/A for files
                    "distinct_regex_matches": distinct_match_count,  # Unique texts
                    "distinct_match_percentage": 0.0,  # N/A for files
                    "column_name_matched": match_stats.column_name_matched if match_stats else False,
                    "words_match_count": match_stats.words_match_count if match_stats else 0,
                    "words_match_rate": 0.0,  # N/A for files
                    "exact_match_count": match_stats.exact_match_count if match_stats else 0,
                    "exact_match_rate": 0.0,  # N/A for files
                    "negative_match_count": match_stats.negative_match_count if match_stats else 0,
                    "negative_match_rate": 0.0  # N/A for files
                }                    
                db_records.append(record)
            return db_records

        # ======================================================================
        # STRUCTURED DATA AGGREGATION LOGIC - UNCHANGED
        # ======================================================================
        else:
            summary_map = defaultdict(lambda: {
                "findings": [],
                "high_tier_rows": set(),
                "medium_tier_rows": set(),
                "low_tier_rows": set()
            })

            for finding in all_findings:
                context = finding.context_data or {}
                key = (finding.classifier_id, context.get('field_name', ''))
                
                summary_map[key]["findings"].append(finding)
                
                # Get tier from finding
                tier = context.get('confidence_tier', 'LOW')
                
                # Get row identifier (use confidence score as fallback if no row_id)
                row_id = context.get("row_identifier", {})
                
                # ✅ FIX: If no row_identifier, create one from row_idx
                if not row_id:
                    row_idx = context.get('row_idx')
                    if row_idx is not None:
                        row_id = {'row_idx': row_idx}
                
                # Convert to tuple for set storage
                row_id_tuple = tuple(sorted(row_id.items())) if row_id else None
                
                # Track by tier
                if row_id_tuple:
                    if tier == 'HIGH':
                        summary_map[key]["high_tier_rows"].add(row_id_tuple)
                    elif tier == 'MEDIUM':
                        summary_map[key]["medium_tier_rows"].add(row_id_tuple)
                    else:
                        summary_map[key]["low_tier_rows"].add(row_id_tuple)

            db_records = []
            for (classifier_id, field_name), data in summary_map.items():
                findings_list = data["findings"]
                first_finding = findings_list[0]
                context = first_finding.context_data or {}
                scores = [f.confidence_score for f in findings_list]
                
                # ✅ NEW: Calculate tier distribution
                high_count = len(data["high_tier_rows"])
                medium_count = len(data["medium_tier_rows"])
                low_count = len(data["low_tier_rows"])
                total_finding_rows = high_count + medium_count + low_count
                
                # Use total_rows_scanned if we have it, otherwise use finding count
                denominator = total_rows_scanned if total_rows_scanned > 0 else total_finding_rows
                
                if denominator > 0:
                    high_percent = (high_count / denominator * 100)
                    medium_percent = (medium_count / denominator * 100)
                    combined_high_medium_percent = ((high_count + medium_count) / denominator * 100)
                else:
                    high_percent = 0
                    medium_percent = 0
                    combined_high_medium_percent = 0
                
                # ✅ NEW: Tier decision logic
                # Priority 1: If >60% HIGH tier → Column is HIGH
                if high_percent > 60:
                    aggregated_tier = "HIGH"
                # Priority 2: If >80% are HIGH or MEDIUM → Column is HIGH
                elif combined_high_medium_percent > 80:
                    aggregated_tier = "HIGH"
                # Priority 3: If >30% HIGH or >50% HIGH+MEDIUM → Column is MEDIUM
                elif high_percent > 30 or combined_high_medium_percent > 50:
                    aggregated_tier = "MEDIUM"
                # Otherwise: Column is LOW
                else:
                    aggregated_tier = "LOW"

                samples = [{"text": f.text, "confidence": f.confidence_score, "row": f.context_data.get("row_identifier")} 
                          for f in findings_list[:self.config.max_samples_per_finding]]
                
                # Extract statistics
                column_stats = context.get('column_statistics')
                match_stats = context.get('match_statistics')
                
                finding_context = {
                    "data_source_id": self.job_context.get("datasource_id"),
                    "classifier_id": classifier_id,
                    "entity_type": first_finding.entity_type,
                    "schema_name": context.get("table_metadata", {}).get("schema_name"),
                    "table_name": context.get("table_name"),
                    "field_name": field_name,
                    "file_path": None,
                    "file_name": None
                } 
                finding_key_hash = generate_finding_key_hash(finding_context)                    
                
                record = {
                    'finding_key_hash': finding_key_hash,
                    "scan_job_id": self.job_context.get("job_id"),
                    "data_source_id": self.job_context.get("datasource_id"),
                    "classifier_id": classifier_id,
                    "entity_type": first_finding.entity_type,
                    "schema_name": context.get("table_metadata", {}).get("schema_name"),
                    "table_name": context.get("table_name"),
                    "field_name": field_name,
                    "file_path": None,
                    "file_name": None,
                    "finding_count": len(findings_list),
                    "average_confidence": sum(scores) / len(scores) if scores else 0,
                    "max_confidence": max(scores) if scores else 0,
                    "confidence_tier": aggregated_tier,
                    "sample_findings": json.dumps(samples),
                    "total_rows_in_source": total_rows_scanned,
                    
                    # Column Statistics
                    "null_percentage": column_stats.null_percentage if column_stats else None,
                    "min_length": column_stats.min_value_length if column_stats else None,
                    "max_length": column_stats.max_value_length if column_stats else None,
                    "mean_length": column_stats.mean_value_length if column_stats else None,
                    "distinct_value_count": column_stats.distinct_value_count if column_stats else None,
                    "distinct_value_percentage": column_stats.distinct_value_percentage if column_stats else None,
                    
                    # Match Statistics
                    "total_regex_matches": match_stats.total_regex_matches if match_stats else 0,
                    "regex_match_rate": match_stats.regex_match_rate if match_stats else 0.0,
                    "distinct_regex_matches": match_stats.distinct_matches if match_stats else 0,
                    "distinct_match_percentage": match_stats.distinct_match_percentage if match_stats else 0.0,
                    "column_name_matched": match_stats.column_name_matched if match_stats else False,
                    "words_match_count": match_stats.words_match_count if match_stats else 0,
                    "words_match_rate": match_stats.words_match_rate if match_stats else 0.0,
                    "exact_match_count": match_stats.exact_match_count if match_stats else 0,
                    "exact_match_rate": match_stats.exact_match_rate if match_stats else 0.0,
                    "negative_match_count": match_stats.negative_match_count if match_stats else 0,
                    "negative_match_rate": match_stats.negative_match_rate if match_stats else 0.0
                }                    
                db_records.append(record)
            
            return db_records


    async def classify_database_table(
        self, 
        table_data: List[Dict[str, Any]], 
        table_metadata: Dict[str, Any]
    ) -> List[PIIFinding]:
        """
        Classifies an entire database table using strategy-based processing.
        
        This is the entry point for database table classification. It:
        1. Selects the appropriate processing strategy
        2. Gets the list of active classifiers
        3. Delegates to RowProcessor.process_table()
        
        Args:
            table_data: List of row dictionaries
            table_metadata: Table schema information
            
        Returns:
            List of PIIFinding objects with full statistics
        """
        if not self._engine or not self._row_processor:
            raise ProcessingError("EngineInterface not initialized.", ErrorType.SYSTEM_INTERNAL_ERROR)
        
        try:
            # Select processing strategy based on classifier requirements
            strategy = self._select_processing_strategy()
            
            # Get active classifiers from engine
            active_classifiers = list(self._engine.classifier_configs.values())
            
            self.logger.info(
                f"Starting table classification with strategy: {strategy.value}",
                table_name=table_metadata.get("table_name"),
                row_count=len(table_data),
                classifier_count=len(active_classifiers),
                **self.job_context
            )
            
            # Delegate to RowProcessor
            findings = await self._row_processor.process_table(
                table_data=table_data,
                table_metadata=table_metadata,
                strategy=strategy,
                classifier_engine=self._engine,
                active_classifiers=active_classifiers
            )
            
            self.logger.info(
                f"Table classification completed",
                findings_count=len(findings),
                **self.job_context
            )
            
            return findings
            
        except Exception as e:
            context = {
                **self.job_context,
                "table_name": table_metadata.get("table_name"),
                "schema_name": table_metadata.get("schema_name")
            }
            self.logger.error(
                "Table classification failed",
                exc_info=True,
                **context
            )
            self.error_handler.handle_error(e, "classify_database_table", **context)
            return []


    async def classify_table_component(
        self,
        component: ContentComponent
    ) -> List[PIIFinding]:
        """
        Classifies a table extracted from a file (PDF/Word/Excel) with quality assessment.
        
        Flow:
        1. Assess table quality (row count, column consistency)
        2. If PASS → Use strategy-based table processing
        3. If FAIL → Fall back to text-based processing
        
        Args:
            component: ContentComponent with component_type="table"
            
        Returns:
            List of PIIFinding objects
        """
        if not self._engine or not self._row_processor:
            raise ProcessingError("EngineInterface not initialized.", ErrorType.SYSTEM_INTERNAL_ERROR)
        
        try:
            # Extract table data from component
            table_schema = component.schema
            
            if not table_schema or not isinstance(table_schema, dict):
                self.logger.warning(
                    "Table component missing valid schema, falling back to text processing",
                    component_id=component.component_id,
                    **self.job_context
                )
                # Fall back to text processing
                file_metadata = {
                    "file_path": component.parent_path,
                    "file_name": component.parent_path.split('/')[-1] if component.parent_path else component.component_id,
                    "component_type": component.component_type,
                    "component_id": component.component_id
                }
                return await self.classify_document_content(
                    content=component.content or "",
                    file_metadata=file_metadata
                )
            
            # Step 1: Assess table quality
            quality_passed = self._assess_table_quality(table_schema)
            
            if not quality_passed:
                self.logger.info(
                    "Table failed quality assessment, using text-based processing",
                    component_id=component.component_id,
                    row_count=table_schema.get("row_count", 0),
                    **self.job_context
                )
                # Serialize table to text and process as document
                serialized_content = table_schema.get("serialized_rows", "")
                file_metadata = {
                    "file_path": component.parent_path,
                    "file_name": component.parent_path.split('/')[-1] if component.parent_path else component.component_id,
                    "component_type": "table_fallback",
                    "component_id": component.component_id,
                    "quality_failed": True
                }
                return await self.classify_document_content(
                    content=serialized_content,
                    file_metadata=file_metadata
                )
            
            # Step 2: Table passed quality - use strategy-based processing
            self.logger.info(
                "Table passed quality assessment, using strategy-based processing",
                component_id=component.component_id,
                row_count=table_schema.get("row_count", 0),
                **self.job_context
            )
            
            # Convert table schema to format expected by process_table()
            headers = table_schema.get("headers", [])
            rows = table_schema.get("rows", [])
            
            # Build table_metadata
            table_metadata = {
                "table_name": component.component_id,
                "columns": {header: {"data_type": "VARCHAR"} for header in headers},
                "source_file": component.parent_path,
                "component_type": component.component_type,
                "extraction_method": component.extraction_method,
                "schema_name": None
            }
            
            # Get strategy and active classifiers
            strategy = self._select_processing_strategy()
            active_classifiers = list(self._engine.classifier_configs.values())
            
            # Process table
            findings = await self._row_processor.process_table(
                table_data=rows,
                table_metadata=table_metadata,
                strategy=strategy,
                classifier_engine=self._engine,
                active_classifiers=active_classifiers
            )
            
            # Enrich findings with file context
            for finding in findings:
                if not hasattr(finding, 'context_data') or finding.context_data is None:
                    finding.context_data = {}
                finding.context_data.update({
                    "file_path": component.parent_path,
                    "file_name": component.parent_path.split('/')[-1] if component.parent_path else component.component_id,
                    "component_id": component.component_id,
                    "component_type": component.component_type,
                    "extraction_method": component.extraction_method,
                    "table_quality_passed": True
                })
            
            return findings
            
        except Exception as e:
            context = {
                **self.job_context,
                "component_id": component.component_id,
                "parent_path": component.parent_path
            }
            self.logger.error(
                "Table component classification failed",
                exc_info=True,
                **context
            )
            self.error_handler.handle_error(e, "classify_table_component", **context)
            return []


    def _assess_table_quality(self, table_schema: Dict[str, Any]) -> bool:
        """
        Assess if extracted table meets quality thresholds.
        
        Uses hard rules from configuration:
        - Minimum row count
        - Minimum column consistency percentage
        
        Args:
            table_schema: The schema dict from ContentComponent
            
        Returns:
            True if table passes all quality checks
        """
        try:
            # Get thresholds from config
            min_row_count = getattr(
                self.config, 
                'table_quality_min_row_count', 
                50  # Default from design doc
            )
            min_consistency_percent = getattr(
                self.config,
                'table_quality_min_column_consistency_percent',
                95  # Default from design doc
            )
            
            # Check 1: Minimum row count
            row_count = table_schema.get('row_count', 0)
            if row_count < min_row_count:
                self.logger.debug(
                    f"Table failed quality check: insufficient rows ({row_count} < {min_row_count})",
                    **self.job_context
                )
                return False
            
            # Check 2: Column consistency
            headers = table_schema.get('headers', [])
            rows = table_schema.get('rows', [])
            
            if not headers or not rows:
                self.logger.debug(
                    "Table failed quality check: missing headers or rows",
                    **self.job_context
                )
                return False
            
            # Count rows with consistent column structure
            consistent_rows = sum(
                1 for row in rows 
                if isinstance(row, dict) and set(row.keys()) == set(headers)
            )
            
            consistency_percent = (consistent_rows / row_count * 100) if row_count > 0 else 0
            
            if consistency_percent < min_consistency_percent:
                self.logger.debug(
                    f"Table failed quality check: low column consistency ({consistency_percent:.1f}% < {min_consistency_percent}%)",
                    **self.job_context
                )
                return False
            
            # All checks passed
            self.logger.debug(
                f"Table passed quality checks: {row_count} rows, {consistency_percent:.1f}% consistency",
                **self.job_context
            )
            return True
            
        except Exception as e:
            self.logger.warning(
                f"Table quality assessment failed with error: {str(e)}",
                exc_info=True,
                **self.job_context
            )
            # Default to False on error (fail safe)
            return False


    def _select_processing_strategy(self) -> ProcessingStrategy:
        """
        Select processing strategy based on classifier requirements.
        
        Logic:
        - If ANY classifier has requires_row_context=true → MULTI_ROW_WITH_NAMES
        - Otherwise → COLUMNAR (best performance)
        
        Returns:
            ProcessingStrategy enum value
        """
        try:
            # Get active classifiers
            active_classifiers = list(self._engine.classifier_configs.values())
            
            # Check if any classifier requires row context
            requires_row_context = any(
                classifier.get('requires_row_context', False)
                for classifier in active_classifiers
            )
            
            if requires_row_context:
                strategy = ProcessingStrategy.MULTI_ROW_WITH_NAMES
                self.logger.debug(
                    "Selected MULTI_ROW_WITH_NAMES strategy (row context required)",
                    **self.job_context
                )
            else:
                strategy = ProcessingStrategy.COLUMNAR
                self.logger.debug(
                    "Selected COLUMNAR strategy (no row context required)",
                    **self.job_context
                )
            
            return strategy
            
        except Exception as e:
            self.logger.warning(
                f"Strategy selection failed, defaulting to MULTI_ROW_WITH_NAMES: {str(e)}",
                exc_info=True,
                **self.job_context
            )
            # Default to safe strategy on error
            return ProcessingStrategy.MULTI_ROW_WITH_NAMES



# =============================================================================
# DEMONSTRATION OF USAGE
# This shows how a Worker would use this class to process a task.
# =============================================================================
if __name__ == "__main__":
    from unittest.mock import MagicMock
    # Import mocked ORM models for the example
    from ..db_models.classifiertemplate_schema import ClassifierTemplate as OrmClassifierTemplate
    from ..db_models.classifier_schema import Classifier as OrmClassifier
    
    # --- 1. SETUP: This would happen once at the start of a worker process ---
    # Create mock core services
    mock_logger = MagicMock(spec=SystemLogger)
    mock_error_handler = MagicMock(spec=ErrorHandler)
    mock_db_interface = MagicMock(spec=DatabaseInterface)
    mock_config_manager = MagicMock(spec=ConfigurationManager)
    # Configure the config manager with default settings for our test
    mock_config_manager.settings.classification.confidence_scoring = ClassificationConfidenceConfig()
    mock_config_manager.settings.classification.max_samples_per_finding = 5

    # --- 2. CONFIGURE MOCK DATABASE RESPONSE ---
    # This simulates what `get_classifier_template_full` would return.
    mock_orm_template_result = OrmClassifierTemplate(
        template_id="pii_v1", name="PII Template", configuration='{}',
        classifiers=[
            OrmClassifier(
                classifier_id="test_ssn", entity_type="US_SSN", name="Test SSN",
                patterns=[MagicMock(name="ssn", regex=r"\d{9}", score=0.8)],
                context_rules=[], validation_rules=[], exclude_list=[]
            )
        ]
    )
    mock_db_interface.get_classifier_template_full.return_value = mock_orm_template_result

    # --- 3. A WORKER RECEIVES A TASK ---
    # The worker receives a work packet with the job context.
    job_context = {
        "job_id": "job_abc_123",
        "task_id": "task_def_456",
        "datasource_id": "ds_xyz_789",
        "trace_id": "trace_112233"
    }
    template_to_use = "pii_v1"

    async def run_worker_task():
        # 4. WORKER CREATES AND INITIALIZES THE INTERFACE
        print("--- Worker is initializing the EngineInterface for the job ---")
        interface = EngineInterface(
            db_interface=mock_db_interface,
            config_manager=mock_config_manager,
            system_logger=mock_logger,
            error_handler=mock_error_handler,
            job_context=job_context
        )
        interface.initialize_for_template(template_to_use)

        # 5. WORKER PROCESSES DATA AND ACCUMULATES FINDINGS
        print("\n--- Worker is processing rows and accumulating findings ---")
        all_findings_for_table = []
        
        # Mock processing two rows from a table
        rows_to_process = [
            {"row_data": {"id": 1, "data": "ssn is 123456789"}, "row_pk": {"id": 1}},
            {"row_data": {"id": 2, "data": "no pii here"}, "row_pk": {"id": 2}},
        ]
        mock_table_meta = {"table_name": "users", "columns": {"id": {}, "data": {}}}

        for row in rows_to_process:
            findings = await interface.classify_database_row(
                row_data=row["row_data"],
                row_pk=row["row_pk"],
                table_metadata=mock_table_meta
            )
            if findings:
                all_findings_for_table.extend(findings)
                print(f"Found {len(findings)} finding(s) in row {row['row_pk']['id']}")

        # 6. WORKER CONVERTS FINDINGS FOR DATABASE STORAGE
        print("\n--- Worker is converting all findings into DB format ---")
        db_ready_records = interface.convert_findings_to_db_format(
            all_findings=all_findings_for_table,
            total_rows_scanned=len(rows_to_process)
        )

        # 7. WORKER SAVES THE RESULTS
        print("\n--- Worker is calling the database interface to save results ---")
        # In a real app, you would call:
        # mock_db_interface.insert_scan_findings(db_ready_records, context=job_context)
        print("Final DB Records Payload:")
        print(json.dumps(db_ready_records, indent=2))
    
    # Run the simulated worker task
    asyncio.run(run_worker_task())
