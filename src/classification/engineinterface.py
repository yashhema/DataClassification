"""
The primary, unified interface for the entire classification subsystem.

This class encapsulates all the internal logic for configuration loading,
engine instantiation, data processing, and results formatting, providing
a clean, simple API to the rest of the application (e.g., the Orchestrator).
"""

import asyncio
import json
import hashlib
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict

# Import from project structure
from .engine import ClassificationEngine
from .row_processor import RowProcessor
from core.db.database_interface import DatabaseInterface
from core.config.configuration_manager import ConfigurationManager, ClassificationConfidenceConfig
from core.logging.system_logger import SystemLogger
from core.errors import ErrorHandler, ClassificationError, ErrorType
from core.models.models import PIIFinding

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

    def load_and_assemble(self, template_id: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        orm_template = self.db_interface.get_classifier_template_full(template_id, context=self.job_context)
        if not orm_template:
            raise ClassificationError(f"Template '{template_id}' not found.", ErrorType.CONFIGURATION_MISSING)

        nested_config = json.loads(orm_template.configuration or '{}')
        template_config = {
            "template_id": orm_template.template_id,
            "name": orm_template.name,
            "classifiers": [{"classifier_id": c.classifier_id} for c in orm_template.classifiers],
            **nested_config
        }

        classifier_configs = {}
        for orm_classifier in orm_template.classifiers:
            classifier_configs[orm_classifier.classifier_id] = {
                "classifier_id": orm_classifier.classifier_id, "entity_type": orm_classifier.entity_type,
                "name": orm_classifier.name,
                "patterns": [{"name": p.name, "regex": p.regex, "score": p.score} for p in orm_classifier.patterns],
                "context_rules": [{"rule_type": p.rule_type.value, "regex": p.regex, "window_before": p.window_before, "window_after": p.window_after} for p in orm_classifier.context_rules],
                "validation_rules": [{"validation_fn_name": v.validation_fn_name} for v in orm_classifier.validation_rules],
                # Assumes an 'exclude_list' relationship exists on the ORM model
                "exclude_list": [e.term_to_exclude for e in getattr(orm_classifier, 'exclude_list', [])]
            }
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

    def initialize_for_template(self, template_id: str):
        """
        Initializes the internal engine and processor for a specific template.
        Must be called before any classification methods.
        """
        try:
            self.logger.info(f"Initializing EngineInterface for template '{template_id}'", **self.job_context)
            loader = _ConfigurationLoader(self.db_interface, self.job_context)
            template_config, classifier_configs = loader.load_and_assemble(template_id)
            
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
            context = {**self.job_context, "row_pk": str(row_pk)}
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
            self.error_handler.handle_error(e, "classify_document_content", **context)
            return []

    def convert_findings_to_db_format(self, all_findings: List[PIIFinding], total_rows_scanned: int) -> List[Dict[str, Any]]:
            """
            Converts a list of PIIFinding objects from a data object into the
            aggregated dictionary format required for database storage.

            It applies different aggregation logic for structured (database) and
            unstructured (file) sources.

            Args:
                all_findings: A list of all PIIFinding objects from a single data object.
                total_rows_scanned: For structured data, the total rows processed. For
                                    unstructured data, this should be the total number of
                                    components processed within the file.

            Returns:
                A list of dictionaries, each ready for `insert_scan_findings`.
            """
            if not all_findings:
                return []

            # --- Determine Source Type from the first finding's context ---
            first_finding_context = all_findings[0].context_data or {}
            is_file_source = 'file_path' in first_finding_context

            # ======================================================================
            # FILE AGGREGATION LOGIC (NEW)
            # Goal: One summary record per classifier for the entire file.
            # ======================================================================
            if is_file_source:
                # Group all findings from all components by classifier_id
                summary_map = defaultdict(list)
                for finding in all_findings:
                    summary_map[finding.classifier_id].append(finding)
                
                db_records = []
                for classifier_id, findings_list in summary_map.items():
                    first_finding = findings_list[0]
                    context = first_finding.context_data or {}
                    scores = [f.confidence_score for f in findings_list]
                    
                    # --- Aggregated Confidence Score Logic for Files ---
                    # A file is "HIGH" if it contains any high-confidence findings.
                    high_conf_score = self.config.confidence_scoring.high_confidence_min_score
                    has_high_confidence_finding = any(s >= high_conf_score for s in scores)
                    avg_confidence = sum(scores) / len(scores) if scores else 0
                    
                    agg_conf = "LOW"
                    if has_high_confidence_finding:
                        agg_conf = "HIGH"
                    elif avg_confidence > (self.config.confidence_scoring.low_threshold / 100):
                         agg_conf = "MEDIUM"

                    samples = [{"text": f.text, "confidence": f.confidence_score} for f in findings_list[:self.config.max_samples_per_finding]]

                    record = {
                        "ScanJobID": self.job_context.get("job_id"),
                        "DataSourceID": self.job_context.get("datasource_id"),
                        "ClassifierID": classifier_id,
                        "EntityType": first_finding.entity_type,
                        "SchemaName": None,
                        "TableName": None,
                        "FieldName": None,  # Set to NULL for file-level summary
                        "FilePath": context.get("file_path"),
                        "FileName": context.get("file_name"),
                        "FindingCount": len(findings_list),
                        "AverageConfidence": avg_confidence,
                        "MaxConfidence": max(scores) if scores else 0,
                        "SampleFindings": json.dumps(samples),
                        "TotalRowsInSource": total_rows_scanned, # Represents component count for files
                        "AggregatedConfidence": agg_conf
                    }
                    db_records.append(record)
                return db_records

            # ======================================================================
            # STRUCTURED DATA AGGREGATION LOGIC (EXISTING)
            # Goal: One summary record per classifier per field (column).
            # ======================================================================
            else:
                summary_map = defaultdict(lambda: {"findings": [], "high_confidence_rows": set()})
                high_conf_score = self.config.confidence_scoring.high_confidence_min_score

                for finding in all_findings:
                    context = finding.context_data or {}
                    key = (finding.classifier_id, context.get('field_name', ''))
                    
                    summary_map[key]["findings"].append(finding)
                    if finding.confidence_score >= high_conf_score:
                        row_id_tuple = tuple(sorted(context.get("row_identifier", {}).items()))
                        if row_id_tuple:
                            summary_map[key]["high_confidence_rows"].add(row_id_tuple)

                db_records = []
                for (classifier_id, field_name), data in summary_map.items():
                    findings_list = data["findings"]
                    first_finding = findings_list[0]
                    context = first_finding.context_data or {}
                    scores = [f.confidence_score for f in findings_list]
                    
                    # --- Aggregated Confidence Score Logic for Structured Data ---
                    high_conf_rows = len(data["high_confidence_rows"])
                    confidence_percent = (high_conf_rows / total_rows_scanned * 100) if total_rows_scanned > 0 else 0
                    
                    agg_conf = "LOW"
                    if confidence_percent > self.config.confidence_scoring.medium_threshold:
                        agg_conf = "HIGH"
                    elif confidence_percent > self.config.confidence_scoring.low_threshold:
                        agg_conf = "MEDIUM"

                    samples = [{"text": f.text, "confidence": f.confidence_score, "row": f.context_data.get("row_identifier")} for f in findings_list[:self.config.max_samples_per_finding]]
                    
                    record = {
                        "ScanJobID": self.job_context.get("job_id"),
                        "DataSourceID": self.job_context.get("datasource_id"),
                        "ClassifierID": classifier_id,
                        "EntityType": first_finding.entity_type,
                        "SchemaName": context.get("table_metadata", {}).get("schema_name"),
                        "TableName": context.get("table_name"),
                        "FieldName": field_name,
                        "FilePath": None, "FileName": None,
                        "FindingCount": len(findings_list),
                        "AverageConfidence": sum(scores) / len(scores) if scores else 0,
                        "MaxConfidence": max(scores) if scores else 0,
                        "SampleFindings": json.dumps(samples),
                        "TotalRowsInSource": total_rows_scanned,
                        "AggregatedConfidence": agg_conf 
                    }
                    db_records.append(record)
            return db_records

# =============================================================================
# DEMONSTRATION OF USAGE
# This shows how a Worker would use this class to process a task.
# =============================================================================
if __name__ == "__main__":
    from unittest.mock import MagicMock
    # Import mocked ORM models for the example
    from ..db_models.classifiertemplate_schema import ClassifierTemplate as OrmClassifierTemplate
    from ..db_models.classifier_schema import Classifier as OrmClassifier, RuleType
    
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
