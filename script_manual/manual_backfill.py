import os
import sys
import json
import requests
import hashlib
import jwt
import time
import uuid
import psycopg2
import re
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

# --- 1. AUTH FUNCTIONS (Matched strictly to api_ingestion.py) ---

def clean_pem(env_key):
    if not env_key: return None
    key = env_key.strip().strip('"').strip("'").replace("\\n", "\n")
    if "-----BEGIN" in key and "\n" not in key:
        key = key.replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n")
        key = key.replace("-----END PRIVATE KEY-----", "\n-----END PRIVATE KEY-----")
    return key

def get_dott_token():
    pem_key = clean_pem(os.getenv("DOTT_PRIVATE_KEY"))
    if not pem_key: return ""
    now = int(time.time())
    payload = {
        "iss": f"{os.getenv('DOTT_ORGANIZATION_ID')}@external.organization.ridedott.com",
        "aud": "https://mds.api.ridedott.com",
        "jti": str(uuid.uuid4()), "iat": now, "exp": now + 3600 
    }
    return jwt.encode(payload, pem_key, algorithm="ES256", headers={"kid": os.getenv("DOTT_KID")})

def get_voi_token():
    url = os.getenv("VOI_AUTH_URL", "https://api.voiapp.io/v1/token")
    user = os.getenv("VOI_USER_ID")
    pw = os.getenv("VOI_PASSWORD")
    try:
        res = requests.post(url, auth=(user, pw), data={'grant_type': 'client_credentials'}, timeout=10)
        res.raise_for_status()
        return res.json().get("access_token")
    except Exception as e:
        print(f"Failed Voi Auth: {e}")
        return ""

def get_bolt_token():
    # BOLT FIX: Dynamic Username/Password login based on your api_ingestion.py
    url = os.getenv("BOLT_AUTH_URL", "https://mds.bolt.eu/auth")
    headers = {"Content-Type": "application/json", "Accept": "application/vnd.mds+json;version=2.0"}
    payload = {"user_name": os.getenv("BOLT_USER"), "user_pass": os.getenv("BOLT_PASSWORD")}
    try:
        # verify=False added to match your api_ingestion.py
        res = requests.post(url, headers=headers, json=payload, verify=False, timeout=20)
        res.raise_for_status()
        return res.json().get("access_token")
    except Exception as e:
        print(f"Failed Bolt Auth: {e}")
        return ""

# --- 2. DATABASE CONNECTION ---
def get_connection():
    raw_host = os.getenv("PG_HOST")
    db_host = "localhost" if raw_host == "host.docker.internal" else raw_host
    return psycopg2.connect(
        host=db_host, port=os.getenv("PG_PORT", "5432"),
        database=os.getenv("PG_DATABASE", "stage_micromobility"),
        user=os.getenv("PG_USER"), password=os.getenv("PG_PASS")
    )

# --- 3. PROVIDER CONFIGURATION & ENDPOINT MAP ---
PROVIDER_CONFIGS = {
    'voi': {
        'base_url': lambda: f"{os.getenv('VOI_MDS_URL')}/{os.getenv('VOI_ZONE_ID')}",
        'auth': lambda: f"Bearer {get_voi_token()}",
        'h_type': lambda ep: "application/vnd.mds+json",
        'version': lambda ep: "2.0"
    },
    'dott': {
        'base_url': lambda: f"https://mds.api.ridedott.com/{os.getenv('DOTT_REGION', 'brussels')}",
        'auth': lambda: f"Bearer {get_dott_token()}",
        'h_type': lambda ep: "application/vnd.mds+json" if ep in ['vehicles', 'events'] else "application/vnd.mds.provider+json",
        'version': lambda ep: "1.2" if ep in ['vehicles', 'events'] else "2.0"
    },
    'bolt': {
        'base_url': lambda: os.getenv('BOLT_MDS_URL', 'https://mds.bolt.eu'),
        'auth': lambda: f"Bearer {get_bolt_token()}",
        'h_type': lambda ep: "application/vnd.mds+json",
        'version': lambda ep: "1.2" if ep == "status_changes" else "2.0"
    }
}

# MAP FIX: Strict mapping based on micromobility_dags.py and init_db.py
TARGETS = {
    'voi': {'trips': 'VOI_TRIPS', 'events/historical': 'VOI_EVENTS'},
    'dott': {'trips': 'DOTT_TRIPS', 'events': 'DOTT_EVENTS'},
    'bolt': {'trips': 'BOLT_TRIPS', 'status_changes': 'BOLT_STATUS_CHANGES'}
}

def get_request_params(endpoint, target_dt):
    hour_str = target_dt.strftime("%Y-%m-%dT%H")
    if "historical" in endpoint or endpoint == "status_changes":
        return {"event_time": hour_str}
    elif "trips" in endpoint:
        return {"end_time": hour_str}
    elif endpoint == "events": # Dott specific millisecond logic from api_ingestion
        return {
            "start_time": int((target_dt - timedelta(hours=1)).timestamp() * 1000),
            "end_time": int(target_dt.timestamp() * 1000)
        }
    return {}

# --- 4. BACKFILL ENGINE ---
def run_backfill(start_date, end_date):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    # Disable InsecureRequestWarning for Bolt
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    current_time = start_date
    while current_time < end_date:
        hour_str = current_time.strftime("%Y-%m-%dT%H")
        print(f"\n🚀 === {hour_str} ===")

        for provider, endpoints in TARGETS.items():
            cfg = PROVIDER_CONFIGS.get(provider)
            
            for actual_ep, table_name in endpoints.items():
                headers = {
                    "Authorization": cfg['auth'](),
                    "Accept": f"{cfg['h_type'](actual_ep)};version={cfg['version'](actual_ep)}"
                }
                
                url = f"{cfg['base_url']()}/{actual_ep}"
                params = get_request_params(actual_ep, current_time)
                page = 1

                while url:
                    try:
                        # Only send params on the first request; 'next' URL includes them
                        res = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
                        
                        if res.status_code in [403, 404, 501]: 
                            print(f"  [-] {provider.upper()} {actual_ep}: No data/Bypassed ({res.status_code})")
                            break
                        res.raise_for_status()
                        data = res.json()

                        # Check if data payload exists
                        items = data.get('data', {}).get(actual_ep.split('/')[-1], data) if isinstance(data, dict) else data
                        if not items:
                            break

                        content_str = json.dumps(data, sort_keys=True)
                        md5 = hashlib.md5(content_str.encode('utf-8')).hexdigest()

                        cur.execute(f'SELECT 1 FROM "MICROMOBILITY_RAW"."{table_name}" WHERE md5_hash = %s', (md5,))
                        if cur.fetchone():
                            print(f"  [-] {provider.upper()} {actual_ep} (P{page}): Duplicate")
                        else:
                            cur.execute(
                                f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
                                (content_str, f"{provider}_{actual_ep.replace('/','_')}_{hour_str}_p{page}.json", current_time, md5)
                            )
                            print(f"  [+] {provider.upper()} {actual_ep} (P{page}): Inserted")

                        url = data.get('links', {}).get('next') if isinstance(data, dict) else None
                        params = {} # Clear params for paginated loops
                        page += 1
                        
                    except Exception as e:
                        print(f"  [!] {provider.upper()} {actual_ep}: {e}")
                        break

        current_time += timedelta(hours=1)
    cur.close()
    conn.close()

if __name__ == "__main__":
    # --- CONFIGURATION ---
    # Start small to test the fixes!
    START = datetime(2026, 4, 7, 0)
    END = datetime(2026, 4, 21, 20)
    
    run_backfill(START, END)