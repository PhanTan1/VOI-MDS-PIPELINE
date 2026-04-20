import json
import requests
import hashlib
import time
import uuid
import jwt
import re
import urllib3
import logging
from datetime import datetime, timedelta, timezone
from cryptography.hazmat.primitives import serialization

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. PROVIDER REGISTRY (Mirroring your exact conditional logic) ---
PROVIDER_CONFIGS = {
    'voi': {
        'base_url': lambda: f"{Variable.get('VOI_MDS_URL')}/{Variable.get('VOI_ZONE_ID')}",
        'version': lambda ep: "2.0",
        'auth_func': lambda: f"Bearer {get_voi_token()}",
        'headers_type': lambda ep: "application/vnd.mds+json"
    },
    'dott': {
        'base_url': lambda: f"https://mds.api.ridedott.com/{Variable.get('DOTT_REGION', 'brussels')}",
        'version': lambda ep: "1.2" if ep == 'vehicles' else "2.0",
        'auth_func': lambda: f"Bearer {get_dott_token()}",
        # Mirroring your: if v == "1.2" else f"application/vnd.mds.provider+json"
        'headers_type': lambda ep: "application/vnd.mds+json" if ep == 'vehicles' else "application/vnd.mds.provider+json"
    },
    'bolt': {
        'base_url': lambda: "https://mds.bolt.eu",
        'version': lambda ep: "1.2" if ep == "status_changes" else "2.0",
        'auth_func': lambda: f"Bearer {get_bolt_token()}",
        'headers_type': lambda ep: "application/vnd.mds+json"
    },
    'poppy': {
        'base_url': lambda: "https://poppy.red/mds",
        'version': lambda ep: "2.0",
        'auth_func': lambda: Variable.get("POPPY_API_KEY", None),
        'auth_header': "external-api-key",
        'headers_type': lambda ep: "application/vnd.mds+json"
    }
}

# --- 2. AUTHENTICATION HELPERS (Your exact original code) ---
def get_voi_token():
    res = requests.post(Variable.get("VOI_AUTH_URL"), 
                         auth=(Variable.get("VOI_USER_ID"), Variable.get("VOI_PASSWORD")),
                         data={'grant_type': 'client_credentials'})
    res.raise_for_status()
    return res.json().get("access_token")

def get_bolt_token():
    url = Variable.get("BOLT_AUTH_URL", "https://mds.bolt.eu/auth")
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

# --- 3. THE SMART INGESTOR ---
def extract_and_load(provider, endpoint, table_name, **kwargs):
    # Using utcnow to match your original script
    now = datetime.utcnow()
    
    # 1. Exact Timing Logic from original
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
        params["end_time"] = target_time_str # Mirroring your: ?end_time=2026-04-17T21
    elif "recent" in endpoint:
        params["start_time"] = int((target_dt - timedelta(hours=1)).timestamp() * 1000)
        params["end_time"] = int(target_dt.timestamp() * 1000)

    # 2. Registry Routing (Functional Mirror)
    config = PROVIDER_CONFIGS[provider]
    url = f"{config['base_url']()}/{endpoint}"
    version = config['version'](endpoint)
    h_type = config['headers_type'](endpoint)
    
    # Mirroring your exact header construction
    headers = {
        config.get('auth_header', 'Authorization'): config['auth_func'](),
        "Accept": f"{h_type};version={version}"
    }
    if provider == 'poppy':
        headers["User-Agent"] = "Mozilla/5.0"

    # 3. Execution & Bronze Storage
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    
    if response.status_code in [403, 404, 501]:
        logging.warning(f"Bypassing {provider} {endpoint}: API {response.status_code}")
        return

    response.raise_for_status()
    data = response.json()
    content_str = json.dumps(data, sort_keys=True)
    current_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
    pg_hook = PostgresHook(postgres_conn_id='postgres_raw')
    
    last_hash = pg_hook.get_first(f'SELECT md5_hash FROM "MICROMOBILITY_RAW"."{table_name}" ORDER BY file_ts DESC LIMIT 1')
    
    if not (last_hash and last_hash[0] == current_hash):
        # Mirroring your: file_time logic
        file_time = now.strftime("%Y%m%d") if 'poppy' in provider and 'trips' in endpoint else target_time_str
        pg_hook.run(
            f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
            parameters=(content_str, f"{provider}_{endpoint.replace('/', '_')}_{file_time}.json", now, current_hash)
        )