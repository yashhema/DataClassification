import json
import sys
import os
import argparse

# Use regex module like Presidio does
try:
    import regex as re
    REGEX_MODULE = True
except ImportError:
    import re
    REGEX_MODULE = False
    print("WARNING: 'regex' module not found. Install with: pip install regex")
    print("Presidio uses 'regex' module, so you should too!")


def convert_java_regex_to_python(java_regex):
    """
    Convert Java 8 regex pattern to Python regex pattern.
    
    Key conversions:
    1. Named groups: (?<n>...) to (?P<n>...)
    2. Character class dash fixes: All patterns with â€"-X or â€"-X
    
    Args:
        java_regex: String containing Java regex pattern
        
    Returns:
        python_regex: Converted pattern
    """
    python_regex = java_regex
    
    # 1. Convert named groups from Java (?<n>...) to Python (?P<n>...)
    # Make sure we don't convert lookbehinds (?<! or (?<=)
    python_regex = re.sub(r'\(\?<([^!=][^>]*?)>', r'(?P<\1>', python_regex)
    
    # 2. Fix ALL character class dash patterns
    # The issue: em-dash or en-dash followed by hyphen creates invalid ranges
    
    # Em-dash patterns
    replacements = [
        ('\u2014-]', '\u2014\\-]'),
        ('\u2014-\\', '\u2014\\-\\'),
        ('\u2014-@', '\u2014\\-@'),
        ('\u2014--', '\u2014\\-\\-'),
        ('\u2014- ', '\u2014\\- '),
        ('\u2014-.', '\u2014\\-.'),
    ]
    
    # En-dash patterns
    replacements.extend([
        ('\u2013-]', '\u2013\\-]'),
        ('\u2013-\\', '\u2013\\-\\'),
        ('\u2013-@', '\u2013\\-@'),
        ('\u2013--', '\u2013\\-\\-'),
        ('\u2013- ', '\u2013\\- '),
        ('\u2013-.', '\u2013\\-.'),
    ])
    
    for old, new in replacements:
        python_regex = python_regex.replace(old, new)
    
    return python_regex


def test_regex_compilation(pattern):
    """
    Test if a regex pattern compiles.
    Uses 'regex' module if available (like Presidio), otherwise uses 're'.
    
    Returns:
        Tuple of (compiles: bool, error_message: str or None)
    """
    try:
        re.compile(pattern)
        return True, None
    except re.error as e:
        return False, str(e)
    except Exception as e:
        return False, str(e)


def escape_sql_string(text):
    """
    Escape a string for SQL Server.
    In SQL Server, single quotes are escaped by doubling them.
    
    Args:
        text: String to escape
        
    Returns:
        Escaped string safe for SQL Server
    """
    # SQL Server uses '' to escape single quotes
    return text.replace("'", "''")


def determine_table_name(filename):
    """
    Determine the database table name from the input filename.
    
    Args:
        filename: Input JSON filename
        
    Returns:
        Table name string
    """
    basename = os.path.basename(filename)
    
    # Remove 'updated_' prefix if present
    if basename.startswith('updated_'):
        basename = basename[8:]
    
    # Remove 'failed_' prefix if present
    if basename.startswith('failed_'):
        basename = basename[7:]
    
    # Remove .json extension
    if basename.endswith('.json'):
        basename = basename[:-5]
    
    return basename


def generate_sql_script(json_file, output_sql_file=None):
    """
    Generate SQL Server UPDATE statements from the updated JSON file.
    
    Args:
        json_file: Path to updated_*.json file
        output_sql_file: Optional output SQL file path
        
    Returns:
        Path to generated SQL file
    """
    # Read JSON data
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found: {json_file}")
        return None
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return None
    
    # Determine table name
    table_name = determine_table_name(json_file)
    
    # Generate output filename if not provided
    if output_sql_file is None:
        directory = os.path.dirname(json_file) or '.'
        output_sql_file = os.path.join(directory, f'update_{table_name}.sql')
    
    # Generate SQL statements
    sql_lines = []
    
    # Header
    sql_lines.append('-- =====================================================')
    sql_lines.append(f'-- SQL Server UPDATE statements for {table_name}')
    sql_lines.append(f'-- Generated from: {os.path.basename(json_file)}')
    sql_lines.append(f'-- Total records: {len(data)}')
    sql_lines.append('-- =====================================================')
    sql_lines.append('')
    sql_lines.append('-- IMPORTANT: Review and test these statements in a development')
    sql_lines.append('-- environment before running in production!')
    sql_lines.append('')
    sql_lines.append('BEGIN TRANSACTION;')
    sql_lines.append('')
    
    # Stats
    pass_count = sum(1 for item in data if item.get('pass_fail') == 'PASS')
    fail_count = sum(1 for item in data if item.get('pass_fail') == 'FAIL')
    
    sql_lines.append(f'-- Summary: {pass_count} PASS, {fail_count} FAIL')
    sql_lines.append('')
    
    # Generate UPDATE statements
    for idx, item in enumerate(data, 1):
        record_id = item.get('record_id')
        updated_regex = item.get('updated_regex')
        pass_fail = item.get('pass_fail', 'UNKNOWN')
        
        if updated_regex is None:
            sql_lines.append(f'-- WARNING: Record {record_id} has no updated_regex')
            sql_lines.append('')
            continue
        
        # Escape the regex for SQL
        escaped_regex = escape_sql_string(updated_regex)
        
        # Add comment
        sql_lines.append(f'-- Record {idx}/{len(data)} | ID: {record_id} | Status: {pass_fail}')
        
        # Add UPDATE statement
        sql_lines.append(f'UPDATE {table_name}')
        sql_lines.append(f"SET regex = '{escaped_regex}'")
        sql_lines.append(f'WHERE id = {record_id};')
        sql_lines.append('')
    
    # Footer
    sql_lines.append('-- =====================================================')
    sql_lines.append('-- Review the updates above before committing')
    sql_lines.append('-- =====================================================')
    sql_lines.append('')
    sql_lines.append('-- If everything looks correct, commit the transaction:')
    sql_lines.append('COMMIT TRANSACTION;')
    sql_lines.append('')
    sql_lines.append('-- Or to rollback:')
    sql_lines.append('-- ROLLBACK TRANSACTION;')
    
    # Write SQL file
    with open(output_sql_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(sql_lines))
    
    return output_sql_file


def process_file(input_filename, generate_sql=False):
    """
    Process a JSON file and convert regexes.
    
    Output format:
    [
        {
            "record_id": 1234,
            "original_regex": "...",
            "updated_regex": "...",
            "pass_fail": "PASS" or "FAIL"
        },
        ...
    ]
    
    Args:
        input_filename: Input JSON file path
        generate_sql: Whether to generate SQL script after conversion
        
    Returns:
        Dictionary with statistics
    """
    # Generate output filename
    directory = os.path.dirname(input_filename)
    if directory == '':
        directory = '.'
    basename = os.path.basename(input_filename)
    
    output_filename = os.path.join(directory, f'updated_{basename}')
    
    print(f"\nProcessing: {input_filename}")
    print(f"Output will be saved to: {output_filename}")
    print(f"Using: {'regex' if REGEX_MODULE else 're'} module")
    if not REGEX_MODULE:
        print("⚠️  WARNING: Presidio uses 'regex' module. Install it for full compatibility!")
    print("-" * 80)
    
    # Read input
    try:
        with open(input_filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found: {input_filename}")
        return None
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return None
    
    # Process each record
    output_data = []
    stats = {
        'total': 0,
        'pass': 0,
        'fail': 0
    }
    
    for item in data:
        record_id = item.get('record_id')
        original_regex = item.get('failed_regex')
        
        if original_regex is None:
            print(f"  Warning: Record {record_id} has no 'failed_regex' field")
            continue
        
        stats['total'] += 1
        
        # Convert the regex
        updated_regex = convert_java_regex_to_python(original_regex)
        
        # Test compilation
        compiles, error = test_regex_compilation(updated_regex)
        
        # Determine pass/fail
        pass_fail = "PASS" if compiles else "FAIL"
        
        if compiles:
            stats['pass'] += 1
            status = "✓ PASS"
        else:
            stats['fail'] += 1
            status = f"✗ FAIL: {error}"
        
        print(f"  Record {record_id}: {status}")
        
        # Build output record with exact format requested
        output_record = {
            "record_id": record_id,
            "original_regex": original_regex,
            "updated_regex": updated_regex,
            "pass_fail": pass_fail
        }
        
        output_data.append(output_record)
    
    # Write output file
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4, ensure_ascii=False)
    
    # Print summary
    print("\n" + "=" * 80)
    print("CONVERSION SUMMARY")
    print("=" * 80)
    print(f"Total records: {stats['total']}")
    print(f"PASS: {stats['pass']}")
    print(f"FAIL: {stats['fail']}")
    print(f"\n✓ Output saved to: {output_filename}")
    
    # Generate SQL if requested
    if generate_sql and stats['pass'] > 0:
        print("\n" + "-" * 80)
        print("GENERATING SQL SCRIPT")
        print("-" * 80)
        sql_file = generate_sql_script(output_filename)
        if sql_file:
            print(f"✓ SQL script saved to: {sql_file}")
    
    if not REGEX_MODULE:
        print("\n" + "!" * 80)
        print("IMPORTANT: Install 'regex' module for Presidio compatibility")
        print("  pip install regex")
        print("  Then use: import regex as re  (just like Presidio does)")
        print("!" * 80)
    
    return stats


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Convert Java 8 regex patterns to Python and optionally generate SQL UPDATE statements.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert regex patterns only
  python %(prog)s failed_classifier_patterns.json
  
  # Convert and generate SQL script
  python %(prog)s failed_classifier_patterns.json --sql
  
  # Generate SQL from existing updated file
  python %(prog)s --sql-only updated_failed_classifier_patterns.json
        """
    )
    
    parser.add_argument('input_file', 
                        help='Input JSON file (failed_*.json or updated_*.json)')
    
    parser.add_argument('--sql', '--generate-sql', 
                        action='store_true',
                        dest='generate_sql',
                        help='Generate SQL UPDATE statements after conversion')
    
    parser.add_argument('--sql-only', 
                        action='store_true',
                        dest='sql_only',
                        help='Only generate SQL from existing updated_*.json file (skip conversion)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_file):
        print(f"ERROR: File not found: {args.input_file}")
        sys.exit(1)
    
    # SQL-only mode
    if args.sql_only:
        print("\n" + "=" * 80)
        print("SQL GENERATION MODE")
        print("=" * 80)
        sql_file = generate_sql_script(args.input_file)
        if sql_file:
            print(f"\n✓ SQL script generated: {sql_file}")
            sys.exit(0)
        else:
            sys.exit(1)
    
    # Convert mode (with optional SQL generation)
    stats = process_file(args.input_file, generate_sql=args.generate_sql)
    
    if stats:
        if stats['fail'] == 0:
            print("\n🎉 All regexes converted successfully!")
            sys.exit(0)
        else:
            print(f"\n⚠️  {stats['fail']} regex(es) failed to compile.")
            sys.exit(1)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()