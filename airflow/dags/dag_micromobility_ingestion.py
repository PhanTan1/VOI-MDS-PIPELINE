import json
import requests
import hashlib
import time
import uuid
import jwt
import re
import urllib3
import logging
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization

# --- 0. IMMEDIATE WARNING SUPPRESSION ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from airflow import DAG
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
    # Suppress warnings inside the task execution
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    now = datetime.utcnow()
    # 4-hour delay ensures data is archived on the provider side
    delay_hours = 4 if "historical" in endpoint or "trips" in endpoint or "status_changes" in endpoint else 1
    target_dt = now - timedelta(hours=delay_hours)
    target_time_str = target_dt.strftime("%Y-%m-%dT%H")
    
    params = {}
    
    # --- DYNAMIC PARAMETER ROUTING ---
    if provider == 'poppy' and 'trips' in endpoint:
        params["start_time"] = (now - timedelta(days=2)).strftime("%Y-%m-%d")
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

    # --- API ROUTING & HEADERS ---
    if provider == 'voi':
        token = get_voi_token()
        url = f"{Variable.get('VOI_MDS_URL')}/{Variable.get('VOI_ZONE_ID')}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.mds+json;version=2.0"}
    elif provider == 'dott':
        token = get_dott_token()
        url = f"https://mds.api.ridedott.com/{Variable.get('DOTT_REGION', 'brussels')}/{endpoint}"
        
        # FIX: Force version 1.2 for the vehicles registry, otherwise use 2.0
        if endpoint == 'vehicles':
            headers = {
                "Authorization": f"Bearer {token}", 
                "Accept": "application/vnd.mds+json;version=1.2"
            }
        else:
            headers = {
                "Authorization": f"Bearer {token}", 
                "Accept": "application/vnd.mds.provider+json;version=2.0"
            }
    elif provider == 'bolt':
        token = get_bolt_token()
        url = f"https://mds.bolt.eu/{endpoint}"
        v = "1.2" if endpoint == "status_changes" else "2.0"
        headers = {"Authorization": f"Bearer {token}", "Accept": f"application/vnd.mds+json;version={v}"}
    elif provider == 'poppy':
        api_key = Variable.get("POPPY_API_KEY", None)
        url = f"https://poppy.red/mds/{endpoint}"
        headers = {"external-api-key": api_key, "Accept": "application/vnd.mds+json;version=2.0"}

    # --- PAGINATION LOOP ---
    url_to_fetch = url
    current_params = params
    page_num = 1

    while url_to_fetch:
        response = requests.get(url_to_fetch, headers=headers, params=current_params, verify=False, timeout=30)
        
        if response.status_code in [403, 404, 501]: 
            logging.warning(f"Bypassing {provider} {endpoint}: API returned {response.status_code}")
            break
            
        response.raise_for_status()
        data = response.json()

        # Deduplication & DB Load
        content_str = json.dumps(data, sort_keys=True)
        current_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
        pg_hook = PostgresHook(postgres_conn_id='postgres_raw')
        
        skip_insert = False
        try:
            # Look at the last inserted hash. If it matches exactly, we skip to avoid duplicate identical pages
            last_hash_record = pg_hook.get_first(f'SELECT md5_hash FROM "MICROMOBILITY_RAW"."{table_name}" ORDER BY file_ts DESC LIMIT 1')
            if last_hash_record and last_hash_record[0] == current_hash: 
                skip_insert = True
        except Exception: 
            pass 

        if not skip_insert:
            pg_hook.run(
                f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
                # Added page_num to the filename to ensure unique naming in raw storage
                parameters=(content_str, f"{provider}_{endpoint.replace('/', '_')}_{target_time_str}_p{page_num}.json", datetime.now(), current_hash)
            )
        else:
            logging.info(f"Identical data detected for {provider} {endpoint} page {page_num}. Skipping DB insert.")

        # Check for the next page URL
        # We use an isinstance check because Poppy 'trips' returns a raw list [] rather than a dict {}
        links = data.get("links", {}) if isinstance(data, dict) else {}
        next_url = links.get("next")
        
        if next_url:
            url_to_fetch = next_url
            current_params = None # The `next` URL provided by Dott/MDS already contains the parameters/cursor
            page_num += 1
        else:
            break # Exit loop when no further pages exist

# --- 3. DAG STRUCTURE ---

PROVIDERS = ['voi', 'dott', 'bolt', 'poppy']

with DAG(
    'micromobility_unified_ingestion',
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    # --- FIX: RE-ADDED SHARED CONFIGURATION (Resolves NameError) ---
    project_cfg = ProjectConfig("/opt/airflow/voi_dbt")
    profile_cfg = ProfileConfig(
        profile_name="voi_mds", 
        target_name="prod",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_raw", 
            profile_args={"schema": "MICROMOBILITY_STAGING"} 
        )
    )

    ENDPOINTS = {
        'voi': {
            'vehicles': 'VOI_VEHICLES', 'trips': 'VOI_TRIPS', 
            'vehicles/status': 'VOI_VEHICLES_STATUS', 'events/historical': 'VOI_EVENTS'
        },
        'dott': {
            'vehicles': 'DOTT_VEHICLES', 'trips': 'DOTT_TRIPS', 
            'vehicles/status': 'DOTT_VEHICLES_STATUS', 
            'events/historical': 'DOTT_EVENTS', 
            'telemetry': 'DOTT_TELEMETRY'
        },
        'bolt': {
            'vehicles': 'BOLT_VEHICLES', 'trips': 'BOLT_TRIPS',
            'events/historical': 'BOLT_EVENTS', 
            'status_changes': 'BOLT_STATUS_CHANGES'
        },
        'poppy': {
            'trips/brussels': 'POPPY_TRIPS', 'free_bike_status': 'POPPY_FREE_BIKE_STATUS',
            'vehicle_types': 'POPPY_VEHICLE_TYPES', 'system_information': 'POPPY_SYSTEM_INFORMATION'
        }
    }

    with TaskGroup("bronze_layer") as bronze_group:
        for provider, tasks in ENDPOINTS.items():
            with TaskGroup(f"ingest_{provider}") as provider_group:
                for ep, table in tasks.items():
                    PythonOperator(
                        task_id=f'fetch_{ep.replace("/", "_")}',
                        python_callable=extract_and_load,
                        op_kwargs={'provider': provider, 'endpoint': ep, 'table_name': table}
                    )

    extraction_bridge = EmptyOperator(task_id="extraction_bridge", trigger_rule=TriggerRule.ALL_DONE)

    provider_splinters = {}
    for provider in PROVIDERS:
        provider_splinters[provider] = DbtTaskGroup(
            group_id=f"silver_{provider}_transformation",
            project_config=project_cfg,
            profile_config=profile_cfg,
            render_config=RenderConfig(
                select=[f"path:models/staging/stg_{provider}*"], 
                test_behavior=TestBehavior.AFTER_EACH
            )
        )

    gold_layer = DbtTaskGroup(
        group_id="gold_marts",
        project_config=project_cfg,
        profile_config=profile_cfg,
        render_config=RenderConfig(select=["path:models/marts"], test_behavior=TestBehavior.AFTER_ALL)
    )

    bronze_group >> extraction_bridge
    for provider in PROVIDERS:
        extraction_bridge >> provider_splinters[provider] >> gold_layer