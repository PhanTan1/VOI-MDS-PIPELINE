import os
import requests
import json
import jwt
import time
import uuid
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from pathlib import Path

# --- 0. ENVIRONMENT SETUP ---
# Look for .env in the parent directory (root)
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
env_path = project_root / ".env"

if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
else:
    print(f"Critical Error: .env file not found at {env_path}")
    exit(1)

# --- 1. AUTHENTICATION HELPERS ---

def get_dott_token():
    # Retrieve and clean the key (handles newlines and quotes)
    raw_key = os.getenv("DOTT_PRIVATE_KEY", "").strip().strip('"').strip("'")
    
    # Fix PEM formatting if newlines were lost during env loading
    if "-----BEGIN PRIVATE KEY-----" in raw_key and "\n" not in raw_key:
        header, footer = "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----"
        content = raw_key.replace(header, "").replace(footer, "").replace(" ", "")
        pem_key = f"{header}\n" + "\n".join(re.findall(r'.{1,64}', content)) + f"\n{footer}"
    else:
        pem_key = raw_key

    private_key = serialization.load_pem_private_key(pem_key.encode(), password=None)
    
    now = int(time.time())
    payload = {
        "iss": f"{os.getenv('DOTT_ORGANIZATION_ID')}@external.organization.ridedott.com",
        "aud": "https://mds.api.ridedott.com",
        "jti": str(uuid.uuid4()), 
        "iat": now, 
        "exp": now + 3600 
    }
    return jwt.encode(payload, private_key, algorithm="ES256", headers={"kid": os.getenv("DOTT_KID")})

# --- 2. DOWNLOAD LOGIC ---

def fetch_hour(hour_str):
    token = get_dott_token()
    region = os.getenv("DOTT_REGION", "brussels")
    base_url = f"https://mds.api.ridedott.com/{region}/telemetry"
    
    headers = {
        "Authorization": f"Bearer {token}", 
        "Accept": "application/vnd.mds.provider+json;version=2.0"
    }
    
    # Path for saving local JSON files
    output_dir = script_dir / "downloads"
    output_dir.mkdir(exist_ok=True)
    
    results = []
    current_url = base_url
    params = {"telemetry_time": hour_str}
    page = 1

    print(f"[*] Fetching: {hour_str}")
    
    while current_url:
        try:
            # On first page use params, on subsequent pages the 'next' URL has them
            res = requests.get(current_url, headers=headers, params=params if page == 1 else None, timeout=30)
            res.raise_for_status()
            data = res.json()
            
            results.extend(data.get("telemetry", []))
            current_url = data.get("links", {}).get("next")
            page += 1
        except Exception as e:
            print(f"[!] Error {hour_str} P{page}: {e}")
            break

    # Save output
    file_path = output_dir / f"telemetry_{hour_str}.json"
    with open(file_path, 'w') as f:
        json.dump(results, f)
    
    print(f"[+] Saved {hour_str}: {len(results)} pings")

# --- 3. MAIN LOOP ---

if __name__ == "__main__":
    # Range based on your specific comparison request
    start_dt = datetime(2026, 4, 15, 10)
    end_dt = datetime(2026, 4, 16, 10)
    
    hours = []
    curr = start_dt
    while curr <= end_dt:
        hours.append(curr.strftime("%Y-%m-%dT%H"))
        curr += timedelta(hours=1)

    print(f"Starting download of {len(hours)} hours to {script_dir}/downloads...")
    
    # 5 parallel threads is usually safe for Dott's rate limits
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(fetch_hour, hours)

    print("\n--- Done ---")