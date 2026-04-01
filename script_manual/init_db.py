import os
import sys
import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
from pathlib import Path

# --- Environment Setup ---
# Resolve the absolute path to the root .env file
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
env_path = project_root / ".env"

if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"Environment: Loaded .env from {env_path}")
else:
    print(f"Critical Error: .env file not found at {env_path}")
    sys.exit(1)

# --- Configuration Mapping ---
RAW_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DATABASE", "stage_micromobility")
DB_USER = os.getenv("PG_USER", "tan")
DB_PASS = os.getenv("PG_PASS")

# Logic: Switch host to localhost if running manually outside of Docker
if RAW_HOST == "host.docker.internal":
    DB_HOST = "localhost"
else:
    DB_HOST = RAW_HOST

def get_connection(dbname=None):
    """Utility to create a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=dbname or DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def create_database():
    """Create the target database if it does not already exist."""
    print(f"Connecting to {DB_HOST} to check for database '{DB_NAME}'...")
    conn = get_connection("postgres") 
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        cur.execute(f'CREATE DATABASE "{DB_NAME}"')
        print(f"Database '{DB_NAME}' created successfully.")
    except errors.DuplicateDatabase:
        print(f"Database '{DB_NAME}' already exists.")
    except Exception as e:
        print(f"Error during database creation: {e}")
    finally:
        cur.close()
        conn.close()

def setup_infrastructure():
    """Create uppercase schemas and tables with specific audit columns."""
    conn = get_connection(DB_NAME)
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        # --- PHASE 1: Schema Creation ---
        print("Initializing schemas in uppercase...")
        schemas = [
            "PROD_MICROMOBILITY_RAW",
            "PROD_MICROMOBILITY_STAGING",
            "PROD_MICROMOBILITY_ANALYTICS"
        ]
        for schema in schemas:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        print(f"Schemas verified: {', '.join(schemas)}")

        # --- PHASE 2: Table Creation (VOI MDS 2.0) ---
        # Structure: content (jsonb), filename (varchar), file_ts (timestamp), load_ts (timestamp)
        # No surrogate ID column as requested.
        voi_tables = ["VOI_TRIPS", "VOI_VEHICLES", "VOI_VEHICLES_STATUS", "VOI_EVENTS"]

        print("Creating tables in PROD_MICROMOBILITY_RAW...")
        for table in voi_tables:
            query = f'''
                CREATE TABLE IF NOT EXISTS "PROD_MICROMOBILITY_RAW"."{table}" (
                    content JSONB,
                    filename VARCHAR(255),
                    file_ts TIMESTAMP,
                    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            '''
            cur.execute(query)
            print(f"Table verified: {table}")

        print("Infrastructure setup completed successfully.")

    except Exception as e:
        print(f"Error during infrastructure setup: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("--- STARTING DATABASE INITIALIZATION ---")
    print(f"Target Host: {DB_HOST} | Port: {DB_PORT} | User: {DB_USER}")
    
    create_database()
    setup_infrastructure()
    
    print("--- INITIALIZATION FINISHED ---")