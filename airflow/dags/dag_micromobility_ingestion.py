import json
import requests
import hashlib
import time
import uuid
import jwt
import re
import urllib3 # Added for warning suppression
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization

from airflow import DAG
# --- Future-proof for Airflow 3.0 ---
try:
    from airflow.sdk import Variable, TaskGroup
except ImportError:
    from airflow.models import Variable
    from airflow.utils.task_group import TaskGroup

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.task.trigger_rule import TriggerRule

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

# Disable the noisy warnings about insecure requests from verify=False
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
    # Safely get the URL without relying on Airflow's keyword arguments
    try:
        url = Variable.get("BOLT_AUTH_URL")
    except Exception:
        url = "https://mds.bolt.eu/auth"
    
    # EXACT headers required by Bolt to avoid the 406 error
    headers = {
        "Content-Type": "application/json", 
        "Accept": "application/vnd.mds+json;version=2.0"
    }
    
    # EXACT payload keys from your working version
    payload = {
        "user_name": Variable.get("BOLT_USER"),
        "user_pass": Variable.get("BOLT_PASSWORD") 
    }
    
    # Bypass corporate SSL with verify=False
    res = requests.post(
        url,
        headers=headers,
        json=payload,
        verify=False,
        timeout=20
    )
    
    res.raise_for_status()
    data = res.json()
    
    # Robust token extraction
    return data.get("access_token") or data.get("token") or data.get("data", {}).get("token")

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
    
    # Handle Provider Delays
    if endpoint == "trips":
        delay_hours = 4 if provider == 'dott' else 2
    else:
        delay_hours = 1 
        
    target_dt = now - timedelta(hours=delay_hours)
    target_time_str = target_dt.strftime("%Y-%m-%dT%H")
    
    params = {}

    # Provider Setup
    if provider == 'voi':
        token = get_voi_token()
        url = f"{Variable.get('VOI_MDS_URL')}/{Variable.get('VOI_ZONE_ID')}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.mds+json;version=2.0"}
        if endpoint == "trips":
            params["end_time"] = target_time_str
            
    elif provider == 'dott':
        token = get_dott_token()
        url = f"https://mds.api.ridedott.com/{Variable.get('DOTT_REGION', 'brussels')}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.mds.provider+json;version=2.0"}
        if endpoint == "trips":
            params["end_time"] = target_time_str
            
    elif provider == 'bolt':
        token = get_bolt_token()
        url = f"https://mds.bolt.eu/{endpoint}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.mds+json;version=2.0"}
        
        if endpoint == 'events/recent':
            start_dt = now - timedelta(hours=2)
            params['start_time'] = int(start_dt.timestamp() * 1000)
            params['end_time'] = int(now.timestamp() * 1000)
        elif endpoint == 'trips':
            params["end_time"] = target_time_str

    print(f"Polling {provider} at {url} with params {params}...")
    
    # Bypass corporate SSL inspection for all providers
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    
    # Graceful handling for 404 (Data not ready yet)
    if response.status_code == 404 and endpoint == 'trips':
        print(f"⚠️ 404 Error: Trip data for {target_time_str} is not yet available on {provider}'s server. Skipping this run.")
        return

    response.raise_for_status()
    data = response.json()

    # MD5 Deduplication
    content_str = json.dumps(data, sort_keys=True)
    current_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
    pg_hook = PostgresHook(postgres_conn_id='postgres_raw')
    
    try:
        last_hash_record = pg_hook.get_first(
            f'SELECT md5_hash FROM "MICROMOBILITY_RAW"."{table_name}" ORDER BY file_ts DESC LIMIT 1'
        )
        if last_hash_record and last_hash_record[0] == current_hash:
            print(f"Deduplication: No changes for {provider} {endpoint}. Skipping.")
            return
    except Exception:
        pass 

    # Save to Bronze
    filename = f"{provider}_{endpoint.replace('/', '_')}_{target_time_str}.json"
    pg_hook.run(
        f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
        parameters=(content_str, filename, datetime.now(), current_hash)
    )

# --- 3. DAG STRUCTURE ---

with DAG(
    'micromobility_unified_ingestion',
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    ENDPOINTS = {
        'voi': {'vehicles': 'VOI_VEHICLES', 'trips': 'VOI_TRIPS', 'vehicles/status': 'VOI_VEHICLES_STATUS'},
        'dott': {'vehicles': 'DOTT_VEHICLES', 'trips': 'DOTT_TRIPS', 'vehicles/status': 'DOTT_VEHICLES_STATUS'},
        'bolt': {'vehicles': 'BOLT_VEHICLES','events/recent': 'BOLT_EVENTS', 'trips': 'BOLT_TRIPS' }
    }

    with TaskGroup("bronze_layer") as bronze_group:
        for provider, tasks in ENDPOINTS.items():
            for ep, table in tasks.items():
                PythonOperator(
                    task_id=f'fetch_{provider}_{ep.replace("/", "_")}',
                    python_callable=extract_and_load,
                    op_kwargs={'provider': provider, 'endpoint': ep, 'table_name': table}
                )

    extraction_bridge = EmptyOperator(
        task_id="extraction_bridge",
        trigger_rule=TriggerRule.ALL_DONE
    )

    dbt_group = DbtTaskGroup(
        group_id="transformation",
        project_config=ProjectConfig("/opt/airflow/voi_dbt"),
        profile_config=ProfileConfig(
            profile_name="voi_mds", 
            target_name="prod",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_raw", 
                # FIX: Set this to your actual dbt schema to prevent "ghost backup" relation errors
                profile_args={"schema": "MICROMOBILITY_STAGING"} 
            )
        ),
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL)
    )

    # Extractions >> Bridge >> dbt
    bronze_group >> extraction_bridge >> dbt_group