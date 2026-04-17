import os
import json
import hashlib
import psycopg2
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# --- 0. ENVIRONMENT SETUP ---
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
env_path = project_root / ".env"

if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
else:
    print(f"Critical Error: .env file not found at {env_path}")
    exit(1)

# --- 1. DATABASE CONNECTION ---
def get_connection():
    # Logic: Switch host to localhost if running manually outside of Docker
    raw_host = os.getenv("PG_HOST")
    db_host = "localhost" if raw_host == "host.docker.internal" else raw_host
    
    return psycopg2.connect(
        host=db_host,
        port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DATABASE", "stage_micromobility"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASS")
    )

# --- 2. INSERT LOGIC ---
def insert_files():
    downloads_dir = script_dir / "downloads"
    if not downloads_dir.exists():
        print("Error: 'downloads' folder not found. Run the download script first.")
        return

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    json_files = list(downloads_dir.glob("*.json"))
    print(f"Found {len(json_files)} files to process.")

    for file_path in json_files:
        with open(file_path, 'r') as f:
            raw_data = json.load(f)
            # Re-wrap in the structure dbt expects: {"telemetry": [...]}
            wrapped_data = {"telemetry": raw_data}
            content_str = json.dumps(wrapped_data, sort_keys=True)
            
            # Calculate MD5 for deduplication
            current_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
            
            # Extract timestamp from filename (e.g., telemetry_2026-04-15T10.json)
            # We use this to set the file_ts column
            try:
                ts_part = file_path.stem.split('_')[1]
                file_ts = datetime.strptime(ts_part, "%Y-%m-%dT%H")
            except:
                file_ts = datetime.now()

            # Check if hash already exists to avoid duplicates
            cur.execute(
                'SELECT 1 FROM "MICROMOBILITY_RAW"."DOTT_TELEMETRY" WHERE md5_hash = %s',
                (current_hash,)
            )
            if cur.fetchone():
                print(f"[-] Skipping {file_path.name} (Duplicate hash)")
                continue

            # Insert into database
            try:
                cur.execute(
                    '''
                    INSERT INTO "MICROMOBILITY_RAW"."DOTT_TELEMETRY" 
                    (content, filename, file_ts, md5_hash) 
                    VALUES (%s, %s, %s, %s)
                    ''',
                    (content_str, file_path.name, file_ts, current_hash)
                )
                print(f"[+] Inserted {file_path.name}")
            except Exception as e:
                print(f"[!] Failed to insert {file_path.name}: {e}")

    cur.close()
    conn.close()
    print("\n--- Bulk Load Finished ---")

if __name__ == "__main__":
    insert_files()