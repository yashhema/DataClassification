"""
Enhanced Microsoft Presidio with a full post-processing pipeline to
dynamically apply all database-driven classifier rules.
"""

import re
import threading
from typing import List, Dict, Any, Optional

try:
    from presidio_analyzer import AnalyzerEngine, PatternRecognizer, Pattern
    from presidio_analyzer.nlp_engine import NlpEngineProvider
    from presidio_analyzer.recognizer_registry import RecognizerRegistry
except ImportError as e:
    raise ImportError("presidio-analyzer is required: pip install presidio-analyzer") from e

# Import from project structure
from core.models.models import PIIFinding
from core.errors import ProcessingError, ErrorType, ErrorHandler

class ClassificationEngine:
    """
    A self-contained, thread-safe PII detection engine that fully implements
    the detailed classifier configurations loaded from the database.
    """
    
    def __init__(self, template_config: Dict[str, Any], classifier_configs: Dict[str, Dict[str, Any]], error_handler: ErrorHandler):
        self.template_config = template_config
        self.classifier_configs = classifier_configs
        self.error_handler = error_handler
        self.analyzer: Optional[AnalyzerEngine] = None
        self._initialized = False
        self._lock = threading.RLock()

        # A mapping of validation function names to actual methods
        self._validation_dispatcher = {
            "validate_luhn_algorithm": self._validate_luhn,
            "validate_ssn_nist": self._validate_ssn_nist
            # Add other validation functions here
        }

        self._initialize_engine()

    def _initialize_engine(self):
        """Initializes the Presidio engine with dynamically built recognizers."""
        with self._lock:
            if self._initialized:
                return
            
            try:
                provider = NlpEngineProvider()
                nlp_engine = provider.create_engine()
                registry = RecognizerRegistry()
                
                # Create and add a recognizer for each classifier in the template
                for classifier_ref in self.template_config.get('classifiers', []):
                    classifier_id = classifier_ref['classifier_id']
                    if classifier_id in self.classifier_configs:
                        recognizer = self._create_recognizer_from_config(self.classifier_configs[classifier_id])
                        if recognizer:
                            registry.add_recognizer(recognizer)
                
                self.analyzer = AnalyzerEngine(nlp_engine=nlp_engine, registry=registry)
                self._initialized = True
            except Exception as e:
                raise self.error_handler.handle_error(e, "presidio_initialization")

    def _create_recognizer_from_config(self, config: Dict[str, Any]) -> Optional[PatternRecognizer]:
        """Creates a Presidio PatternRecognizer from a detailed classifier configuration dictionary."""
        try:
            patterns = [Pattern(name=p['name'], regex=p['regex'], score=p['score']) for p in config.get('patterns', [])]
            if not patterns:
                return None

            support_rules = [r for r in config.get('context_rules', []) if r['rule_type'] == 'SUPPORT']
            support_keywords = [r['regex'] for r in support_rules] if support_rules else []
            max_window = max((r['window_before'] + r['window_after']) for r in support_rules) if support_rules else 50

            return PatternRecognizer(
                supported_entity=config['entity_type'],
                name=config['classifier_id'],
                patterns=patterns,
                context=support_keywords,
                context_window_size=max_window
            )
        except Exception:
            # Errors will be logged by the caller
            return None

    def classify_content(self, content: str, context_info: Dict[str, Any]) -> List[PIIFinding]:
        """
        Classifies content using a full post-processing pipeline to apply all rules.
        """
        if not self._initialized or not self.analyzer:
            raise ProcessingError("Engine not initialized", ErrorType.SYSTEM_INTERNAL_ERROR)

        with self._lock:
            try:
                entity_types = [
                    conf['entity_type'] for cid, conf in self.classifier_configs.items()
                    if cid in [c['classifier_id'] for c in self.template_config.get('classifiers', [])]
                ]
                if not entity_types:
                    return []

                # 1. Get initial raw findings from Presidio
                raw_results = self.analyzer.analyze(text=content, entities=entity_types, language='en')
                
                # 2. Run the post-processing pipeline
                processed_findings = []
                for result in raw_results:
                    classifier_config = self.classifier_configs.get(result.recognizer_name)
                    if not classifier_config:
                        continue

                    # The pipeline: if any check fails, the finding is discarded.
                    if not self._check_negative_support(result, content, classifier_config):
                        continue
                    if not self._check_validations(result, content, classifier_config):
                        continue
                    if not self._check_exclude_list(result, content, classifier_config):
                        continue

                    # 3. If all checks pass, create the final PIIFinding object
                    finding = PIIFinding(
                        entity_type=result.entity_type,
                        text=content[result.start:result.end],
                        start_position=result.start,
                        end_position=result.end,
                        confidence_score=result.score, # Can be enhanced later
                        classifier_id=classifier_config['classifier_id'],
                        # Attach the rich context for later use
                        context_data=context_info 
                    )
                    processed_findings.append(finding)

                return processed_findings
            except Exception:
                # Let the interface handle logging and error conversion
                raise

    def _check_negative_support(self, result, content, config) -> bool:
        """Returns False if a negative support keyword is found, True otherwise."""
        negative_rules = [r for r in config.get('context_rules', []) if r['rule_type'] == 'NEGATIVE_SUPPORT']
        for rule in negative_rules:
            start = max(0, result.start - rule['window_before'])
            end = min(len(content), result.end + rule['window_after'])
            if re.search(rule['regex'], content[start:end], re.IGNORECASE):
                return False  # Finding is negated
        return True

    def _check_validations(self, result, content, config) -> bool:
        """Returns False if a required validation fails, True otherwise."""
        validation_rules = config.get('validation_rules', [])
        if not validation_rules:
            return True
        
        text_to_validate = content[result.start:result.end]
        for rule in validation_rules:
            fn_name = rule['validation_fn_name']
            validator = self._validation_dispatcher.get(fn_name)
            if validator and not validator(text_to_validate):
                return False # Validation failed
        return True

    def _check_exclude_list(self, result, content, config) -> bool:
        """Returns False if the finding text is in the exclude list, True otherwise."""
        exclude_list = config.get('exclude_list', []) # This key needs to be added by the loader
        if not exclude_list:
            return True
        
        found_text = content[result.start:result.end]
        if found_text.lower() in [term.lower() for term in exclude_list]:
            return False # Finding is excluded
        return True

    # --- Specific Validation Functions ---
    def _validate_luhn(self, card_number: str) -> bool:
        digits = [int(d) for d in re.sub(r'\D', '', card_number)]
        if len(digits) < 13: return False
        check_digit = digits.pop()
        digits.reverse()
        doubled = [d * 2 if i % 2 == 0 else d for i, d in enumerate(digits)]
        summed = [d - 9 if d > 9 else d for d in doubled]
        return (sum(summed) + check_digit) % 10 == 0

    def _validate_ssn_nist(self, ssn: str) -> bool:
        digits = re.sub(r'\D', '', ssn)
        if len(digits) != 9: return False
        area, group, serial = digits[:3], digits[3:5], digits[5:]
        if area in ["000", "666"] or area.startswith("9"): return False
        if group == "00" or serial == "0000": return False
        return True
