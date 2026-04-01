import os
import sys
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
from pathlib import Path

# --- Environment Setup ---
project_root = Path(__file__).resolve().parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

DB_HOST = os.getenv("PG_HOST", "localhost") 
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DATABASE", "stage_micromobility")
DB_USER = os.getenv("PG_USER", "tan")
DB_PASS = os.getenv("PG_PASS", "password")

def get_connection(dbname=None):
    """Utility to create a connection to the database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=dbname or DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def create_database():
    """Create the target database if it does not exist."""
    conn = get_connection("postgres") 
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f'CREATE DATABASE "{DB_NAME}"')
        print(f"Database '{DB_NAME}' created successfully.")
    except errors.DuplicateDatabase:
        print(f"Database '{DB_NAME}' already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cur.close()
        conn.close()

def setup_infrastructure():
    """Initialize uppercase schemas and raw tables without a surrogate ID."""
    conn = get_connection(DB_NAME)
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        print("Creating schemas...")
        schemas = [
            "PROD_MICROMOBILITY_RAW",
            "PROD_MICROMOBILITY_STAGING",
            "PROD_MICROMOBILITY_ANALYTICS"
        ]
        for schema in schemas:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

        # MDS 2.0 endpoints mapped to tables
        voi_tables = ["VOI_TRIPS", "VOI_VEHICLES", "VOI_VEHICLES_STATUS", "VOI_EVENTS"]

        print("Creating RAW tables following the standard structure...")
        for table in voi_tables:
            # Table structure without 'id' column to match other providers
            query = f'''
                CREATE TABLE IF NOT EXISTS "PROD_MICROMOBILITY_RAW"."{table}" (
                    content JSONB,
                    filename VARCHAR(255),
                    file_ts TIMESTAMP,
                    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            '''
            cur.execute(query)

        print("Infrastructure setup successfully completed.")

    except Exception as e:
        print(f"Setup error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    create_database()
    setup_infrastructure()