import asyncio
import json
import aioodbc
from datetime import datetime

CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=NewDataClassification;"
    "Trusted_Connection=yes;"
    "MARS_Connection=yes;"
)

async def dump_tables():
    conn = await aioodbc.connect(dsn=CONNECTION_STRING)
    cursor = await conn.cursor()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    try:
        # Dump classifier_patterns
        print("Dumping classifier_patterns...")
        await cursor.execute("""
            SELECT cp.id, cp.classifier_id, cp.name, cp.regex, cp.score, c.classifier_id as clf_string_id
            FROM classifier_patterns cp
            JOIN classifiers c ON cp.classifier_id = c.id
            ORDER BY cp.id
        """)
        
        patterns = await cursor.fetchall()
        patterns_data = [
            {
                'id': row[0],
                'classifier_fk_id': row[1],
                'classifier_id': row[5],
                'name': row[2],
                'regex': row[3],
                'score': row[4]
            }
            for row in patterns
        ]
        
        patterns_file = f"classifier_patterns_backup_{timestamp}.json"
        with open(patterns_file, 'w', encoding='utf-8') as f:
            json.dump(patterns_data, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(patterns_data)} patterns to: {patterns_file}")
        
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
        rules_data = [
            {
                'id': row[0],
                'classifier_fk_id': row[1],
                'classifier_id': row[6],
                'rule_type': row[2],
                'regex': row[3],
                'window_before': row[4],
                'window_after': row[5]
            }
            for row in rules
        ]
        
        rules_file = f"classifier_context_rules_backup_{timestamp}.json"
        with open(rules_file, 'w', encoding='utf-8') as f:
            json.dump(rules_data, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(rules_data)} context rules to: {rules_file}")
        
        print("\nDump complete!")
        
    finally:
        await cursor.close()
        await conn.close()

if __name__ == "__main__":
    asyncio.run(dump_tables())