# test_template_patterns.py
import asyncio
import logging
import regex as re # Use the same regex engine as the classification engine

# Core component imports
from core.config.configuration_manager import ConfigurationManager
from core.db.database_interface import DatabaseInterface
from core.errors import ErrorHandler
from core.logging.system_logger import SystemLogger

# Import the internal loader class from the engine interface
from classification.engineinterface import _ConfigurationLoader

# --- Configuration ---
CONFIG_FILE_PATH = "config/system_default.yaml"

# 1. Set the template you want to test
TEMPLATE_ID_TO_TEST = "unstructured"

# 2. Set the sample data string you want to test the regex patterns against
TEST_STRING = 'FIRSTNAME:PATSY | LASTNAME:AAMODT | FULLNAME:PATSY AAMODT | ADDR:30870 STERLING HWY | CITY:ANCHOR POINT | ST:AK | ZIP:99556 | PHONE:907-236-2793 | BIRTHDAY:1916-01-10 00:00:00 | EMAIL:PATSY_AAMODT@AOL.COM | SSN:574-18-0576 | PASSPORT:243798014 | PASSPORTISSUED:2022-01-10 00:00:00 | PASSPORTEXPIRE:2027-01-10 00:00:00 | DL:8923125 | DLSTATE:AK | DLISSUED:2022-01-10 00:00:00 | DLEXPIRE:2027-01-10 00:00:00 | CC:MASTERCARD | CCNO:5234792011936522 | CCCSV:583 | CCEXPIRE:2022-10-01 00:00:00 | BANK:CREDIT UNION 1 | ROUTING:325272063 | BANKACCT:49222452561 | EIN:13-8019929 | ITIN:969-79-3823 | ATIN:913-93-8965 | PTIN:P42009562 | SIDN:S34415205 | MILITARYID:7508604533 | MEDICARE_MBI:6TU0-HY7-EY08 | HICN:RI-869-32-0295-B'


async def validate_and_test_template():
    """
    Loads a classifier template, validates its regex patterns, and tests each
    pattern against a sample string to show live matches.
    """
    db_interface = None
    print("=" * 80)
    print(f"🔬 TESTING CLASSIFIERS FOR TEMPLATE: '{TEMPLATE_ID_TO_TEST}'")
    print("=" * 80)

    try:
        # --- Initialization ---
        print("\n[1] Initializing core components...")
        logging.basicConfig(level=logging.WARNING)
        error_handler = ErrorHandler()
        temp_logger = SystemLogger(logging.getLogger("validator_startup"), "TEXT", {})
        config_manager = ConfigurationManager(CONFIG_FILE_PATH)
        config_manager.set_core_services(temp_logger, error_handler)

        db_interface = DatabaseInterface(
            config_manager.get_db_connection_string(), temp_logger, error_handler
        )
        await db_interface.test_connection()
        print("    ✓ Database connection successful.")

        # --- Load Configuration ---
        print(f"\n[2] Loading configuration for template '{TEMPLATE_ID_TO_TEST}'...")
        loader = _ConfigurationLoader(db_interface, {"job_id": "template_tester"})
        template_config, classifier_configs = await loader.load_and_assemble(
            TEMPLATE_ID_TO_TEST, temp_logger, error_handler
        )
        print(f"    ✓ Loaded template with {len(template_config.get('classifiers', []))} classifiers.")

        # --- Validation and Live Test Loop ---
        print("\n[3] Validating and testing regex patterns against sample string...")
        print(f"    Test String: \"{TEST_STRING}\"")
        total_patterns = 0
        failed_patterns = 0

        for classifier_ref in template_config.get('classifiers', []):
            classifier_id = classifier_ref['classifier_id']
            config = classifier_configs.get(classifier_id)

            if not config:
                print(f"\n--- ❌ Classifier '{classifier_id}' not found! ---")
                continue

            print(f"\n--- Classifier: {config.get('name')} (ID: {classifier_id}) ---")
            patterns = config.get('patterns', [])
            if not patterns:
                print("    (No patterns to validate)")
                continue

            for pattern in patterns:
                total_patterns += 1
                regex_str = pattern.get('regex')
                pattern_name = pattern.get('name')
                print(f"    - Pattern: '{pattern_name}'")
                try:
                    # Step 1: Validate that the regex can be compiled
                    re.compile(regex_str)
                    print(f"      ✅ VALID Regex: {regex_str[:100]}{'...' if len(regex_str) > 100 else ''}")

                    # Step 2: Run the validated regex against the test string
                    try:
                        
                        matches = re.findall(regex_str, TEST_STRING)
                        if matches:
                            print(f"      🎯 MATCH FOUND: {matches}")
                        else:
                            print(f"      (No match in test string)")
                    except Exception as find_err:
                        print(f"      ❌ RUNTIME ERROR during findall: {find_err}")

                except (re.error, TypeError) as e:
                    # This block catches errors during compilation
                    failed_patterns += 1
                    print(f"      ❌ INVALID Regex: {regex_str}")
                    print(f"         Error: {e}")

    except Exception as e:
        print(f"\nFATAL ERROR: An unexpected error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if db_interface:
            await db_interface.close_async()
            print("\n[INFO] Database connection closed.")

    # --- Final Summary ---
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print(f"  Total Patterns Checked: {total_patterns}")
    if failed_patterns == 0:
        print(f"  🎉 All {total_patterns} patterns compiled successfully!")
    else:
        print(f"  ❌ Found {failed_patterns} invalid regex patterns.")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(validate_and_test_template())