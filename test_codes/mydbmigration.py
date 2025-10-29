# create_schema_only.py
"""
Script to create a new database with snake_case naming from SQLAlchemy models
This version only creates the schema without copying any data.
"""

import logging
import sys
from sqlalchemy import create_engine, text

try:
    # Import your base and all models (running from src directory)
    from core.db_models.base import Base
    
    print("✓ All SQLAlchemy models imported successfully")
    
except ImportError as e:
    print(f"✗ Error importing models: {e}")
    print("Make sure you're running this script from the correct directory and all model files exist.")
    sys.exit(1)

def create_new_database_schema():
    """Create a new database with proper snake_case schema from SQLAlchemy models"""
    
    # Database settings
    old_database = "DataClassification"
    new_database = "NewDataClassification"
    
    print(f"Creating new database: {new_database}")
    print(f"Source reference: {old_database}")
    print("-" * 50)
    
    # Connection strings - using consistent sync pyodbc driver
    master_connection_string = "mssql+pyodbc://localhost/master?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    new_db_connection_string = "mssql+pyodbc://localhost/NewDataClassification?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    
    print("Step 1: Creating new database...")
    
    # Create the new database using sync engine with AUTOCOMMIT isolation
    master_engine = create_engine(master_connection_string, isolation_level="AUTOCOMMIT")
    
    try:
        with master_engine.connect() as conn:
            # Check if database already exists
            result = conn.execute(text(f"SELECT database_id FROM sys.databases WHERE name = '{new_database}'"))
            if result.fetchone():
                print(f"⚠️  Database '{new_database}' already exists")
                drop_existing = input("Do you want to drop and recreate it? (y/n): ")
                if drop_existing.lower() == 'y':
                    # Drop existing database
                    conn.execute(text(f"ALTER DATABASE [{new_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"))
                    conn.execute(text(f"DROP DATABASE [{new_database}]"))
                    print(f"✓ Dropped existing database '{new_database}'")
                else:
                    print("Operation cancelled")
                    return False
            
            # Create new database
            conn.execute(text(f"CREATE DATABASE [{new_database}]"))
            # Note: CREATE DATABASE is auto-commit, no need for explicit commit
            
            # Verify database was created successfully
            result = conn.execute(text(f"SELECT database_id FROM sys.databases WHERE name = '{new_database}'"))
            if not result.fetchone():
                raise Exception(f"Database {new_database} was not created successfully")
            
            print(f"✓ Database '{new_database}' created successfully")
            
    except Exception as e:
        print(f"✗ Error creating database: {e}")
        return False
    finally:
        master_engine.dispose()
    
    print("\nStep 2: Creating tables from SQLAlchemy models...")
    
    # Create sync engine for the new database
    new_db_engine = create_engine(new_db_connection_string, echo=True)
    
    try:
        # Create all tables with proper snake_case naming
        Base.metadata.create_all(new_db_engine)
        print("✓ All tables created successfully with snake_case naming")
        
        # Verify tables were created
        with new_db_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE' 
                ORDER BY TABLE_NAME
            """))
            tables = [row[0] for row in result.fetchall()]
            
            print(f"\n✓ Created {len(tables)} tables:")
            for table in tables:
                print(f"   - {table}")
                
    except Exception as e:
        print(f"✗ Error creating schema: {e}")
        return False
    finally:
        new_db_engine.dispose()
    
    print(f"\n🎉 Schema creation completed successfully!")
    print(f"Database '{new_database}' is ready with snake_case naming convention")
    print(f"Connection string: {new_db_connection_string}")
    
    return True

def verify_schema():
    """Verify the created schema matches expectations"""
    
    # Use consistent sync connection for verification
    connection_string = "mssql+pyodbc://localhost/NewDataClassification?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    engine = create_engine(connection_string)
    
    print("\nStep 3: Verifying schema...")
    
    try:
        with engine.connect() as conn:
            # Check for key tables with snake_case naming
            expected_tables = [
                'calendars', 'calendar_rules', 'classifier_categories', 
                'classifiers', 'classifier_templates', 'datasources',
                'discovered_objects', 'object_metadata', 'scan_finding_summaries',
                'jobs', 'tasks', 'system_parameters'
            ]
            
            for table in expected_tables:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = '{table}'
                """))
                count = result.scalar()
                
                if count > 0:
                    print(f"   ✓ {table}")
                else:
                    print(f"   ✗ {table} - MISSING")
                    
    except Exception as e:
        print(f"✗ Error verifying schema: {e}")
    finally:
        engine.dispose()

def main():
    """Main function to create database schema"""
    
    print("SQLAlchemy Schema Creator")
    print("=" * 50)
    print("This script will create a new database with snake_case naming")
    print("based on your SQLAlchemy models.")
    print()
    
    try:
        # Create the database and schema
        success = create_new_database_schema()
        
        if success:
            # Verify the schema
            verify_schema()
            
            print("\n" + "=" * 50)
            print("✅ SCHEMA CREATION COMPLETE")
            print("\nNext steps:")
            print("1. Update your application's connection string to:")
            print("   mssql+pyodbc://localhost/NewDataClassification?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
            print("   (or use +aioodbc for async operations)")
            print("2. Manually copy any important data from DataClassification to NewDataClassification")
            print("3. Test your application with the new database")
        else:
            print("\n❌ Schema creation failed")
            
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        logging.exception("Schema creation failed")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the schema creation
    main()