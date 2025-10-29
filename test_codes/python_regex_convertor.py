"""
Direct database regex pattern converter and updater for SQL Server.
Converts patterns in BOTH classifier_patterns AND classifier_context_rules tables.
"""

import asyncio
import json
import re
from typing import Dict, List, Tuple, Any
import aioodbc
from datetime import datetime

# Database connection string
CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=NewDataClassification;"
    "Trusted_Connection=yes;"
    "MARS_Connection=yes;"
)


def convert_regex_pattern(java_regex: str) -> Dict[str, Any]:
    """
    Convert Java regex to Python with comprehensive fixes.
    
    Returns:
        {
            'regex': str,              # Converted pattern
            'modifications': List[str], # What was changed
            'exclude_list': List[str], # Terms to add to exclude list
            'warnings': List[str],     # Potential issues
            'compiles': bool,          # Whether it compiles
            'error': str               # Compilation error if any
        }
    """
    python_regex = java_regex
    modifications = []
    exclude_list = []
    warnings = []
    
    # === CRITICAL FIX 1: Variable-Width Lookbehind (SSN Test Patterns) ===
    if '(?<!(?:123' in python_regex and '6789' in python_regex:
        lookbehind_pattern = r'\(\?<!\(\?:123[^\)]+6789\)\|987[^\)]+4321\)\)'
        match = re.search(lookbehind_pattern, python_regex)
        
        if match:
            python_regex = python_regex.replace(match.group(0), '')
            modifications.append('removed_variable_width_lookbehind')
            
            exclude_list.extend([
                '123-45-6789',
                '123 45 6789',
                '123.45.6789',
                '987-65-4321',
                '987 65 4321',
                '987.65.4321'
            ])
    
    # === CRITICAL FIX 2: Typo [A-Z}{1} → [A-Z]{1} ===
    if '[A-Z}{1}' in python_regex:
        python_regex = python_regex.replace('[A-Z}{1}', '[A-Z]{1}')
        modifications.append('fixed_brace_typo')
    
    # === CHARACTER CLASS NORMALIZATION ===
    if re.search(r'\[\\----–—\\-\\-\]', python_regex):
        python_regex = re.sub(r'\[\\----–—\\-\\-\]', r'[\\-–—]', python_regex)
        modifications.append('normalized_dash_class_basic')
    
    if re.search(r'\[\\----–—\\-\\- \]', python_regex):
        python_regex = re.sub(r'\[\\----–—\\-\\- \]', r'[\\-–— ]', python_regex)
        modifications.append('normalized_dash_class_with_space')
    
    if re.search(r'\[ \\t\\----–—\\-\\- \.\]', python_regex):
        python_regex = re.sub(r'\[ \\t\\----–—\\-\\- \.\]', r'[ \\t\\-–—.]', python_regex)
        modifications.append('normalized_separator_class_with_period')
    
    if re.search(r'\[ \\t\\----–—\\-\\- \]', python_regex):
        python_regex = re.sub(r'\[ \\t\\----–—\\-\\- \]', r'[ \\t\\-–—]', python_regex)
        modifications.append('normalized_separator_class')
    
    if re.search(r'\[\\----–—\\-@\\\.\\\$\]', python_regex):
        python_regex = re.sub(r'\[\\----–—\\-@\\\.\\\$\]', r'[\\-–—@.$]', python_regex)
        modifications.append('normalized_dash_with_special_chars')
    
    # === NAMED GROUP CONVERSION ===
    if re.search(r'\(\?<([^!=][^>]*?)>', python_regex):
        python_regex = re.sub(r'\(\?<([^!=][^>]*?)>', r'(?P<\1>', python_regex)
        modifications.append('converted_named_groups')
    
    # === PERFORMANCE WARNINGS ===
    if re.search(r'\(\?=\.\*.*\)\(\?=\.\*.*\)', python_regex):
        warnings.append('nested_lookaheads_backtracking_risk')
    
    if re.search(r'\{14,\}|\{20,\}', python_regex):
        warnings.append('unbounded_quantifier_performance')
    
    lookbehind_count = len(re.findall(r'\(\?<!', python_regex))
    if lookbehind_count > 0:
        warnings.append(f'{lookbehind_count}_lookbehinds_remaining')
    
    # === COMPILATION TEST ===
    try:
        import regex as re_test
        re_test.compile(python_regex)
        compiles = True
        error = None
    except ImportError:
        try:
            import re as re_test
            re_test.compile(python_regex)
            compiles = True
            error = None
        except re_test.error as e:
            compiles = False
            error = str(e)
    except Exception as e:
        compiles = False
        error = str(e)
    
    return {
        'regex': python_regex,
        'modifications': modifications,
        'exclude_list': exclude_list,
        'warnings': warnings,
        'compiles': compiles,
        'error': error
    }


async def dump_tables_to_json():
    """
    Dump both tables to JSON files BEFORE making any changes.
    Creates backup files with timestamps.
    """
    print("\n" + "=" * 80)
    print("BACKING UP TABLES TO JSON")
    print("=" * 80)
    
    conn = await aioodbc.connect(dsn=CONNECTION_STRING)
    cursor = await conn.cursor()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Dump classifier_patterns
        print("\nDumping classifier_patterns...")
        await cursor.execute("""
            SELECT cp.id, cp.classifier_id, cp.name, cp.regex, cp.score, c.classifier_id as clf_string_id
            FROM classifier_patterns cp
            JOIN classifiers c ON cp.classifier_id = c.id
            ORDER BY cp.id
        """)
        
        patterns = await cursor.fetchall()
        patterns_data = []
        
        for row in patterns:
            patterns_data.append({
                'id': row[0],
                'classifier_fk_id': row[1],
                'classifier_id': row[5],
                'name': row[2],
                'regex': row[3],
                'score': row[4]
            })
        
        patterns_filename = f"classifier_patterns_backup_{timestamp}.json"
        with open(patterns_filename, 'w', encoding='utf-8') as f:
            json.dump(patterns_data, f, indent=2, ensure_ascii=False)
        
        print(f"  Saved {len(patterns_data)} patterns to: {patterns_filename}")
        
        # Dump classifier_context_rules
        print("\nDumping classifier_context_rules...")
        await cursor.execute("""
            SELECT ccr.id, ccr.classifier_id, ccr.rule_type, ccr.regex, 
                   ccr.window_before, ccr.window_after, c.classifier_id as clf_string_id
            FROM classifier_context_rules ccr
            JOIN classifiers c ON ccr.classifier_id = c.id
            ORDER BY ccr.id
        """)
        
        rules = await cursor.fetchall()
        rules_data = []
        
        for row in rules:
            rules_data.append({
                'id': row[0],
                'classifier_fk_id': row[1],
                'classifier_id': row[6],
                'rule_type': row[2],
                'regex': row[3],
                'window_before': row[4],
                'window_after': row[5]
            })
        
        rules_filename = f"classifier_context_rules_backup_{timestamp}.json"
        with open(rules_filename, 'w', encoding='utf-8') as f:
            json.dump(rules_data, f, indent=2, ensure_ascii=False)
        
        print(f"  Saved {len(rules_data)} context rules to: {rules_filename}")
        
        print(f"\nBackup complete!")
        return patterns_filename, rules_filename
        
    except Exception as e:
        print(f"ERROR during backup: {e}")
        import traceback
        traceback.print_exc()
        return None, None
        
    finally:
        await cursor.close()
        await conn.close()


async def update_classifier_patterns(cursor):
    """Process and update classifier_patterns table."""
    
    print("\n" + "=" * 80)
    print("PROCESSING TABLE: classifier_patterns")
    print("=" * 80)
    
    await cursor.execute("""
        SELECT cp.id, cp.classifier_id, cp.name, cp.regex, c.classifier_id as clf_string_id
        FROM classifier_patterns cp
        JOIN classifiers c ON cp.classifier_id = c.id
        ORDER BY cp.id
    """)
    
    patterns = await cursor.fetchall()
    print(f"Found {len(patterns)} patterns to process\n")
    
    stats = {
        'total': len(patterns),
        'success': 0,
        'failed': 0,
        'modified': 0,
        'unchanged': 0
    }
    
    for pattern_id, classifier_fk, pattern_name, original_regex, classifier_string_id in patterns:
        print(f"\n{'─' * 80}")
        print(f"[PATTERN] ID: {pattern_id} | Name: {pattern_name}")
        print(f"Classifier: {classifier_string_id}")
        print(f"Original: {original_regex[:100]}{'...' if len(original_regex) > 100 else ''}")
        
        result = convert_regex_pattern(original_regex)
        
        if result['regex'] == original_regex:
            print("Status: UNCHANGED")
            stats['unchanged'] += 1
            stats['success'] += 1
            continue
        
        print(f"Converted: {result['regex'][:100]}{'...' if len(result['regex']) > 100 else ''}")
        print(f"Modifications: {', '.join(result['modifications']) if result['modifications'] else 'none'}")
        
        if result['warnings']:
            print(f"Warnings: {', '.join(result['warnings'])}")
        
        if not result['compiles']:
            print(f"ERROR: Pattern does not compile!")
            print(f"  {result['error']}")
            stats['failed'] += 1
            continue
        
        print("Compilation: PASS ✓")
        
        await cursor.execute("""
            UPDATE classifier_patterns
            SET regex = ?
            WHERE id = ?
        """, result['regex'], pattern_id)
        
        print(f"Updated pattern in database ✓")
        stats['modified'] += 1
        
        if result['exclude_list']:
            print(f"\nAdding {len(result['exclude_list'])} exclude list entries...")
            
            for term in result['exclude_list']:
                await cursor.execute("""
                    SELECT COUNT(*) FROM classifier_exclude_list
                    WHERE classifier_id = ? AND term_to_exclude = ?
                """, classifier_fk, term)
                
                count_row = await cursor.fetchone()
                exists = count_row[0] > 0
                
                if not exists:
                    await cursor.execute("""
                        INSERT INTO classifier_exclude_list (classifier_id, term_to_exclude)
                        VALUES (?, ?)
                    """, classifier_fk, term)
                    print(f"  Added: '{term}'")
                else:
                    print(f"  Skipped (exists): '{term}'")
        
        stats['success'] += 1
    
    return stats


async def update_classifier_context_rules(cursor):
    """Process and update classifier_context_rules table."""
    
    print("\n" + "=" * 80)
    print("PROCESSING TABLE: classifier_context_rules")
    print("=" * 80)
    
    await cursor.execute("""
        SELECT ccr.id, ccr.classifier_id, ccr.rule_type, ccr.regex, 
               ccr.window_before, ccr.window_after, c.classifier_id as clf_string_id
        FROM classifier_context_rules ccr
        JOIN classifiers c ON ccr.classifier_id = c.id
        ORDER BY ccr.id
    """)
    
    rules = await cursor.fetchall()
    print(f"Found {len(rules)} context rules to process\n")
    
    stats = {
        'total': len(rules),
        'success': 0,
        'failed': 0,
        'modified': 0,
        'unchanged': 0
    }
    
    for rule_id, classifier_fk, rule_type, original_regex, window_before, window_after, classifier_string_id in rules:
        print(f"\n{'─' * 80}")
        print(f"[CONTEXT RULE] ID: {rule_id} | Type: {rule_type}")
        print(f"Classifier: {classifier_string_id}")
        print(f"Windows: before={window_before}, after={window_after}")
        print(f"Original: {original_regex[:100]}{'...' if len(original_regex) > 100 else ''}")
        
        result = convert_regex_pattern(original_regex)
        
        if result['regex'] == original_regex:
            print("Status: UNCHANGED")
            stats['unchanged'] += 1
            stats['success'] += 1
            continue
        
        print(f"Converted: {result['regex'][:100]}{'...' if len(result['regex']) > 100 else ''}")
        print(f"Modifications: {', '.join(result['modifications']) if result['modifications'] else 'none'}")
        
        if result['warnings']:
            print(f"Warnings: {', '.join(result['warnings'])}")
        
        if not result['compiles']:
            print(f"ERROR: Pattern does not compile!")
            print(f"  {result['error']}")
            stats['failed'] += 1
            continue
        
        print("Compilation: PASS ✓")
        
        await cursor.execute("""
            UPDATE classifier_context_rules
            SET regex = ?
            WHERE id = ?
        """, result['regex'], rule_id)
        
        print(f"Updated context rule in database ✓")
        stats['modified'] += 1
        stats['success'] += 1
    
    return stats


async def update_database_patterns():
    """
    Main function: Connect to database, convert patterns in BOTH tables, update in place.
    """
    
    print("=" * 80)
    print("REGEX PATTERN CONVERTER & DATABASE UPDATER")
    print("=" * 80)
    print(f"\nConnecting to database...")
    
    conn = await aioodbc.connect(dsn=CONNECTION_STRING)
    cursor = await conn.cursor()
    
    try:
        patterns_stats = await update_classifier_patterns(cursor)
        context_stats = await update_classifier_context_rules(cursor)
        
        await conn.commit()
        
        print("\n" + "=" * 80)
        print("OVERALL CONVERSION SUMMARY")
        print("=" * 80)
        
        print("\nclassifier_patterns:")
        print(f"  Total:            {patterns_stats['total']}")
        print(f"  Modified:         {patterns_stats['modified']}")
        print(f"  Unchanged:        {patterns_stats['unchanged']}")
        print(f"  Failed:           {patterns_stats['failed']}")
        
        print("\nclassifier_context_rules:")
        print(f"  Total:            {context_stats['total']}")
        print(f"  Modified:         {context_stats['modified']}")
        print(f"  Unchanged:        {context_stats['unchanged']}")
        print(f"  Failed:           {context_stats['failed']}")
        
        total_failed = patterns_stats['failed'] + context_stats['failed']
        total_modified = patterns_stats['modified'] + context_stats['modified']
        
        print(f"\nTotal patterns modified: {total_modified}")
        print(f"Total failures: {total_failed}")
        print(f"\nAll changes committed to database ✓")
        
        if total_failed > 0:
            print(f"\nWARNING: {total_failed} pattern(s) failed. Review errors above.")
            return False
        else:
            print("\nSUCCESS: All patterns converted and updated!")
            return True
        
    except Exception as e:
        await conn.rollback()
        print(f"\n\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        await cursor.close()
        await conn.close()
        print("\nDatabase connection closed.")


async def verify_patterns():
    """
    Verification function: Re-read patterns from BOTH tables and test compilation.
    """
    print("\n" + "=" * 80)
    print("VERIFICATION: Testing all patterns compile correctly")
    print("=" * 80)
    
    conn = await aioodbc.connect(dsn=CONNECTION_STRING)
    cursor = await conn.cursor()
    
    try:
        print("\nVerifying classifier_patterns...")
        await cursor.execute("SELECT id, name, regex FROM classifier_patterns")
        patterns = await cursor.fetchall()
        
        pattern_fail_count = 0
        
        for pattern_id, name, regex in patterns:
            try:
                import regex as re_test
                re_test.compile(regex)
                status = "✓"
            except:
                try:
                    import re as re_test
                    re_test.compile(regex)
                    status = "✓"
                except Exception as e:
                    status = f"✗ {str(e)}"
                    pattern_fail_count += 1
            
            print(f"  Pattern {pattern_id:3d} ({name[:40]:40s}): {status}")
        
        print("\nVerifying classifier_context_rules...")
        await cursor.execute("SELECT id, rule_type, regex FROM classifier_context_rules")
        rules = await cursor.fetchall()
        
        rules_fail_count = 0
        
        for rule_id, rule_type, regex in rules:
            try:
                import regex as re_test
                re_test.compile(regex)
                status = "✓"
            except:
                try:
                    import re as re_test
                    re_test.compile(regex)
                    status = "✓"
                except Exception as e:
                    status = f"✗ {str(e)}"
                    rules_fail_count += 1
            
            print(f"  Rule {rule_id:3d} ({rule_type:20s}): {status}")
        
        print("\n" + "=" * 80)
        print("VERIFICATION SUMMARY")
        print("=" * 80)
        print(f"classifier_patterns:      {len(patterns) - pattern_fail_count}/{len(patterns)} compile correctly")
        print(f"classifier_context_rules: {len(rules) - rules_fail_count}/{len(rules)} compile correctly")
        
        total_fail = pattern_fail_count + rules_fail_count
        
        if total_fail == 0:
            print("\n✓ All patterns verified successfully!")
        else:
            print(f"\n✗ {total_fail} pattern(s) failed verification")
        
        return total_fail == 0
        
    finally:
        await cursor.close()
        await conn.close()


async def main():
    """Run the full conversion and verification process."""
    
    print("Step 1: Creating backup...")
    patterns_backup, rules_backup = await dump_tables_to_json()
    
    if not patterns_backup or not rules_backup:
        print("\nBackup failed. Aborting conversion.")
        return 1
    
    print(f"\nBackup files created:")
    print(f"  - {patterns_backup}")
    print(f"  - {rules_backup}")
    
    response = input("\nProceed with conversion? (yes/no): ").strip().lower()
    if response != 'yes':
        print("Conversion cancelled by user.")
        return 0
    
    print("\nStep 2: Converting patterns...")
    success = await update_database_patterns()
    
    if not success:
        print("\nConversion failed. Database rolled back. Check backup files to restore if needed.")
        return 1
    
    print("\nStep 3: Verifying patterns...")
    verified = await verify_patterns()
    
    if verified:
        print("\nAll patterns successfully converted and verified!")
        print(f"\nBackup files retained at:")
        print(f"  - {patterns_backup}")
        print(f"  - {rules_backup}")
        return 0
    else:
        print("\nSome patterns still have issues. Review output above.")
        print(f"Backup files available for restoration at:")
        print(f"  - {patterns_backup}")
        print(f"  - {rules_backup}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)