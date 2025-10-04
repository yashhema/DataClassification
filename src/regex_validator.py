import asyncio
import json
import logging
from typing import List, Optional, Dict, Any

# CRITICAL FIX: Use 'regex' module instead of 're' module
# Presidio uses 'regex' module for advanced regex features
try:
    import regex as re
    REGEX_MODULE = True
    print("✓ Using 'regex' module (Presidio-compatible)")
except ImportError:
    import re
    REGEX_MODULE = False
    print("⚠️  WARNING: Using standard 're' module - install 'regex' for full compatibility!")
    print("   Run: pip install regex")

# Core Component Imports from your project
from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger

# --- Configuration ---
CONFIG_FILE_PATH = "config/system_default.yaml"

def validate_regex(pattern_to_validate: Dict[str, Any], failures: List[Dict[str, Any]]) -> bool:
    """
    Validates a single regex pattern using Python's 'regex' module.
    If it fails, adds the details to the failures list.

    Returns:
        True if the regex is valid, False otherwise.
    """
    regex_pattern = pattern_to_validate.get("regex")
    if not regex_pattern:
        return True  # Treat empty or null as valid.

    try:
        re.compile(regex_pattern)
        return True
    except (re.error, Exception) as e:
        record_id = pattern_to_validate.get("id")
        source_table = pattern_to_validate.get("source_table")
        
        print(f"\n- ✗ INVALID REGEX FOUND in table '{source_table}' (Record ID: {record_id})")
        print(f"  - Error: {e}")
        
        failures.append({
            "record_id": record_id,
            "failed_regex": regex_pattern
        })
        return False

async def main():
    """
    Main function to connect to the DB, fetch all regex patterns,
    validate them, and generate JSON reports for failures.
    """
    print("=" * 60)
    print("Starting Python Regex Pattern Validation Script")
    if REGEX_MODULE:
        print("Using: regex module (Presidio-compatible)")
    else:
        print("Using: re module (LIMITED - may show false failures)")
    print("=" * 60)

    db_interface = None
    failed_primary_patterns = []
    failed_context_rules = []

    try:
        # --- 1. Initialize Core Components & Connect to DB ---
        print("\n[1] Initializing core components and connecting to the database...")
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(message)s')
        
        error_handler = ErrorHandler()
        # Use a basic logger for this script's startup
        temp_logger = SystemLogger(logging.getLogger("validator_startup"), "TEXT", {})
        
        config_manager = ConfigurationManager(CONFIG_FILE_PATH)
        config_manager.set_core_services(temp_logger, error_handler)
        
        db_interface = DatabaseInterface(
            config_manager.get_db_connection_string(),
            temp_logger,
            error_handler
        )
        if not await db_interface.test_connection():
            print("\n✗ DATABASE CONNECTION FAILED. Please check your configuration.")
            return

        print("    ✓ Database connection successful.")

        # --- 2. Fetch all patterns using raw SQL ---
        print("\n[2] Fetching all regex patterns from the database...")
        
        primary_patterns_sql = "SELECT p.id, c.classifier_id, p.regex FROM classifier_patterns p JOIN classifiers c ON p.classifier_id = c.id;"
        context_rules_sql = "SELECT cr.id, c.classifier_id, cr.regex FROM classifier_context_rules cr JOIN classifiers c ON cr.classifier_id = c.id;"

        primary_patterns = await db_interface.execute_raw_sql(primary_patterns_sql) or []
        context_rules = await db_interface.execute_raw_sql(context_rules_sql) or []
        
        # Add source table info for better error messages
        for p in primary_patterns: p['source_table'] = 'classifier_patterns'
        for r in context_rules: r['source_table'] = 'classifier_context_rules'

        print(f"    ✓ Found {len(primary_patterns)} primary patterns and {len(context_rules)} context rules.")

        # --- 3. Validate all patterns ---
        print("\n[3] Running validation on all fetched regex patterns...")
        
        for pattern in primary_patterns:
            validate_regex(pattern, failed_primary_patterns)

        for rule in context_rules:
            validate_regex(rule, failed_context_rules)

    except Exception as ex:
        print(f"\nFATAL ERROR: An unexpected error occurred: {ex}")
        return
    finally:
        if db_interface:
            await db_interface.close_async()

    # --- 4. Generate JSON Output Files ---
    print("\n" + "=" * 60)
    print("VALIDATION COMPLETE & GENERATING OUTPUT")
    print("=" * 60)
    
    try:
        if failed_primary_patterns:
            with open("failed_classifier_patterns.json", "w") as f:
                json.dump(failed_primary_patterns, f, indent=4)
            print(f"  ✗ Found {len(failed_primary_patterns)} invalid patterns in 'classifier_patterns'.")
            print("     -> Output saved to 'failed_classifier_patterns.json'")
        else:
            print("  ✓ All patterns in 'classifier_patterns' are valid.")

        if failed_context_rules:
            with open("failed_classifier_context_rules.json", "w") as f:
                json.dump(failed_context_rules, f, indent=4)
            print(f"  ✗ Found {len(failed_context_rules)} invalid rules in 'classifier_context_rules'.")
            print("     -> Output saved to 'failed_classifier_context_rules.json'")
        else:
            print("  ✓ All rules in 'classifier_context_rules' are valid.")
            
    except Exception as ex:
        print(f"\nERROR: Could not write output files: {ex}")
    finally:
        print("\n" + "=" * 60)
        if not REGEX_MODULE:
            print("⚠️  IMPORTANT: Install 'regex' module for accurate validation!")
            print("   pip install regex")
            print("=" * 60)
        print("Script finished.")
        print("=" * 60)


if __name__ == "__main__":
    # To run this script, navigate to your `src` directory and execute:
    # python regex_validator.py
    asyncio.run(main())