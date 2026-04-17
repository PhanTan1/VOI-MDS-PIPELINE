import json
import requests
import hashlib
import time
import uuid
import jwt
import re
import logging
import urllib3
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization

# Airflow imports needed for the functions to work
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. AUTHENTICATION HELPERS ---
def get_voi_token():
    res = requests.post(
        Variable.get("VOI_AUTH_URL"),
        auth=(Variable.get("VOI_USER_ID"), Variable.get("VOI_PASSWORD")),
        data={'grant_type': 'client_credentials'}
    )
    res.raise_for_status()
    return res.json().get("access_token")

def get_bolt_token():
    try:
        url = Variable.get("BOLT_AUTH_URL")
    except Exception:
        url = "https://mds.bolt.eu/auth"
    headers = {"Content-Type": "application/json", "Accept": "application/vnd.mds+json;version=2.0"}
    payload = {"user_name": Variable.get("BOLT_USER"), "user_pass": Variable.get("BOLT_PASSWORD")}
    res = requests.post(url, headers=headers, json=payload, verify=False, timeout=20)
    res.raise_for_status()
    return res.json().get("access_token")

def get_dott_token():
    raw_key = Variable.get("DOTT_PRIVATE_KEY").strip().strip('"').strip("'")
    if "-----BEGIN PRIVATE KEY-----" in raw_key and "\n" not in raw_key:
        header, footer = "-----BEGIN PRIVATE KEY-----", "-----END PRIVATE KEY-----"
        content = raw_key.replace(header, "").replace(footer, "").replace(" ", "")
        pem_key = f"{header}\n" + "\n".join(re.findall(r'.{1,64}', content)) + f"\n{footer}"
    else:
        pem_key = raw_key
    private_key = serialization.load_pem_private_key(pem_key.encode(), password=None)
    now = int(time.time())
    payload = {
        "iss": f"{Variable.get('DOTT_ORGANIZATION_ID')}@external.organization.ridedott.com",
        "aud": "https://mds.api.ridedott.com",
        "jti": str(uuid.uuid4()), "iat": now, "exp": now + 3600 
    }
    return jwt.encode(payload, private_key, algorithm="ES256", headers={"kid": Variable.get("DOTT_KID")})

# --- 2. THE SMART INGESTOR ---
def extract_and_load(provider, endpoint, table_name, **kwargs):
    now = datetime.utcnow()
    delay_hours = 4 if any(x in endpoint for x in ["historical", "trips", "status_changes"]) else 1
    target_dt = now - timedelta(hours=delay_hours)
    target_time_str = target_dt.strftime("%Y-%m-%dT%H")
    
    params = {}
    
    if provider == 'poppy' and 'trips' in endpoint:
        params["start_time"] = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        params["end_time"] = now.strftime("%Y-%m-%d")
    elif "historical" in endpoint or endpoint == "status_changes":
        params["event_time"] = target_time_str
    elif "telemetry" in endpoint:
        params["telemetry_time"] = target_time_str
    elif "trips" in endpoint:
        params["end_time"] = target_time_str
    elif "recent" in endpoint:
        params["start_time"] = int((target_dt - timedelta(hours=1)).timestamp() * 1000)
        params["end_time"] = int(target_dt.timestamp() * 1000)

    # API Routing
    if provider == 'voi':
        token = get_voi_token()
        url = f"{Variable.get('VOI_MDS_URL')}/{Variable.get('VOI_ZONE_ID')}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.mds+json;version=2.0"}
    elif provider == 'dott':
        token = get_dott_token()
        url = f"https://mds.api.ridedott.com/{Variable.get('DOTT_REGION', 'brussels')}/{endpoint}"
        v = "1.2" if endpoint == 'vehicles' else "2.0"
        headers = {"Authorization": f"Bearer {token}", "Accept": f"application/vnd.mds+json;version={v}" if v == "1.2" else f"application/vnd.mds.provider+json;version={v}"}
    elif provider == 'bolt':
        token = get_bolt_token()
        url = f"https://mds.bolt.eu/{endpoint}"
        v = "1.2" if endpoint == "status_changes" else "2.0"
        headers = {"Authorization": f"Bearer {token}", "Accept": f"application/vnd.mds+json;version={v}"}
    elif provider == 'poppy':
        api_key = Variable.get("POPPY_API_KEY", None)
        url = f"https://poppy.red/mds/{endpoint}"
        headers = {"external-api-key": api_key, "Accept": "application/vnd.mds+json;version=2.0", "User-Agent": "Mozilla/5.0"}

    # Fetching
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    if response.status_code in [403, 404, 501]:
        logging.warning(f"Bypassing {provider} {endpoint}: API {response.status_code}")
        return

    response.raise_for_status()
    data = response.json()
    content_str = json.dumps(data, sort_keys=True)
    current_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
    pg_hook = PostgresHook(postgres_conn_id='postgres_raw')
    
    # Simple dedupe check
    last_hash = pg_hook.get_first(f'SELECT md5_hash FROM "MICROMOBILITY_RAW"."{table_name}" ORDER BY file_ts DESC LIMIT 1')
    if not (last_hash and last_hash[0] == current_hash):
        file_time = now.strftime("%Y%m%d") if 'poppy' in provider and 'trips' in endpoint else target_time_str
        pg_hook.run(
            f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
            parameters=(content_str, f"{provider}_{endpoint.replace('/', '_')}_{file_time}.json", now, current_hash)
        )