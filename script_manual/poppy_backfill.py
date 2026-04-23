import os
import sys
import json
import requests
import hashlib
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# --- 0. ENVIRONMENT SETUP ---
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent 
env_path = project_root / ".env"

if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"✅ Environment loaded from {env_path}")
else:
    print(f"Critical Error: .env file not found at {env_path}")
    sys.exit(1)

# --- 1. DATABASE CONNECTION ---
def get_connection():
    raw_host = os.getenv("PG_HOST")
    db_host = "localhost" if raw_host == "host.docker.internal" else raw_host
    return psycopg2.connect(
        host=db_host, port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DATABASE", "stage_micromobility"),
        user=os.getenv("PG_USER"), password=os.getenv("PG_PASS")
    )

# --- 2. POPPY BACKFILL ENGINE ---
def run_poppy_backfill(start_date, end_date):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    api_key = os.getenv("POPPY_API_KEY")
    if not api_key:
        print("Critical Error: POPPY_API_KEY not found in .env")
        sys.exit(1)

    # Poppy Quirks: Custom Auth Header & Requires User-Agent
    headers = {
        "external-api-key": api_key,
        "Accept": "application/vnd.mds+json;version=2.0",
        "User-Agent": "Mozilla/5.0"
    }

    base_url = "https://poppy.red/mds/trips/brussels"
    table_name = "POPPY_TRIPS"

    # Iterate Day-by-Day (Safe Chunking)
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=1)
        
        # Format as YYYY-MM-DD
        start_str = current_date.strftime("%Y-%m-%d")
        end_str = next_date.strftime("%Y-%m-%d")
        
        print(f"\n🚗 === Fetching POPPY Trips: {start_str} ===")
        
        url = base_url
        params = {
            "start_time": start_str,
            "end_time": end_str
        }
        
        page = 1
        while url:
            try:
                # 60 second timeout because daily chunks can be large
                res = requests.get(url, headers=headers, params=params, timeout=60)
                
                if res.status_code == 404:
                    print("  [-] No data for this day.")
                    break
                res.raise_for_status()
                data = res.json()

                # Extract data payload safely
                items = data.get('data', {}).get('trips', data) if isinstance(data, dict) else data
                if not items:
                    print("  [-] Empty response payload.")
                    break

                # Deduplication Hash
                content_str = json.dumps(data, sort_keys=True)
                md5 = hashlib.md5(content_str.encode('utf-8')).hexdigest()

                cur.execute(f'SELECT 1 FROM "MICROMOBILITY_RAW"."{table_name}" WHERE md5_hash = %s', (md5,))
                if cur.fetchone():
                    print(f"  [-] P{page}: Skip (Duplicate)")
                else:
                    cur.execute(
                        f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
                        (content_str, f"poppy_trips_{start_str}_p{page}.json", current_date, md5)
                    )
                    print(f"  [+] P{page}: Inserted")

                # Handle Pagination
                url = data.get('links', {}).get('next') if isinstance(data, dict) else None
                params = {} # Must clear params because the 'next' URL already contains them
                page += 1

            except Exception as e:
                print(f"  [!] Error: {e}")
                break

        # Move to the next day
        current_date = next_date
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    # --- MANUAL CONFIGURATION ---
    START = datetime(2026, 4, 1)
    END = datetime(2026, 4, 22)
    
    run_poppy_backfill(START, END)