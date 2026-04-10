import os
import sys
import psycopg2
from psycopg2 import sql, errors
from dotenv import load_dotenv
from pathlib import Path

# --- Environment Setup ---
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
DB_HOST = "localhost" if RAW_HOST == "host.docker.internal" else RAW_HOST

def get_connection(dbname=None):
    """Utility to create a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=dbname or DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def check_db_exists(cur, dbname):
    """Check if the database exists in the system catalog."""
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    return cur.fetchone() is not None

def check_extension_exists(cur, extname):
    """Check if the extension is already installed."""
    cur.execute("SELECT 1 FROM pg_extension WHERE extname = %s", (extname,))
    return cur.fetchone() is not None

def create_database():
    """Create the target database only if it does not exist."""
    print(f"Checking for database '{DB_NAME}' on {DB_HOST}...")
    conn = get_connection("postgres") 
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        if not check_db_exists(cur, DB_NAME):
            print(f"Database '{DB_NAME}' not found. Attempting to create...")
            cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(DB_NAME)))
            print(f"Database '{DB_NAME}' created successfully.")
        else:
            print(f"Database '{DB_NAME}' already exists. Skipping creation.")
    except Exception as e:
        print(f"Warning: Could not check/create database: {e}")
    finally:
        cur.close()
        conn.close()

def setup_infrastructure():
    """Setup PostGIS, Schemas, and Unified Tables for all providers."""
    conn = get_connection(DB_NAME)
    conn.autocommit = True 
    cur = conn.cursor()
    
    try:
        # --- PHASE 0: Extension Check ---
        print("Checking for PostGIS extension...")
        if not check_extension_exists(cur, 'postgis'):
            print("PostGIS not found. Attempting to install...")
            try:
                cur.execute('CREATE EXTENSION IF NOT EXISTS postgis;')
                print("PostGIS installed successfully.")
            except errors.InsufficientPrivilege:
                print("Error: You do not have permission to install PostGIS.")
        else:
            print("PostGIS extension already exists.")

        # --- PHASE 1: Schema Creation ---
        schemas = ["MICROMOBILITY_RAW", "MICROMOBILITY_STAGING", "MICROMOBILITY_ANALYTICS"]
        for schema in schemas:
            cur.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
        print(f"Schemas verified: {', '.join(schemas)}")

        # --- PHASE 2: Unified Table Creation & Upgrades ---
        providers = ["VOI", "DOTT", "BOLT"]
        # Included 'EVENTS' specifically to catch the new Bolt MDS 2.0 data
        endpoints = ["TRIPS", "VEHICLES", "VEHICLES_STATUS", "EVENTS"]
        
        for provider in providers:
            for endpoint in endpoints:
                table_name = f"{provider}_{endpoint}"
                
                # 1. Create table if it is brand new
                create_query = sql.SQL('''
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        content JSONB,
                        filename VARCHAR(255),
                        file_ts TIMESTAMP,
                        md5_hash VARCHAR(32), 
                        load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                ''').format(
                    schema=sql.Identifier("MICROMOBILITY_RAW"),
                    table=sql.Identifier(table_name)
                )
                cur.execute(create_query)

                # 2. Upgrade existing tables (The Fix for your Work Computer)
                alter_query = sql.SQL('''
                    ALTER TABLE {schema}.{table}
                    ADD COLUMN IF NOT EXISTS md5_hash VARCHAR(32);
                ''').format(
                    schema=sql.Identifier("MICROMOBILITY_RAW"),
                    table=sql.Identifier(table_name)
                )
                cur.execute(alter_query)
                
                print(f"Table verified & updated: {table_name}")

    except Exception as e:
        print(f"Error during infrastructure setup: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("--- STARTING DATABASE INITIALIZATION ---")
    create_database()
    setup_infrastructure()
    print("--- INITIALIZATION FINISHED ---")