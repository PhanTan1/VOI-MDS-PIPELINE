import json
import requests
import hashlib
import time
import uuid
import jwt
import re
import urllib3
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import serialization

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

# Disable warnings for verify=False
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
    data = res.json()
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
    
    # Custom delay logic for trips vs real-time endpoints
    if "trips" in endpoint:
        delay_hours = 4 if provider == 'dott' else 2
    else:
        delay_hours = 1 
        
    target_dt = now - timedelta(hours=delay_hours)
    target_time_str = target_dt.strftime("%Y-%m-%dT%H")
    
    # --- Universal Parameter Routing based on Postman Specs ---
    params = {}
    if endpoint in ["trips", "trips/brussels"]:
        params["end_time"] = target_time_str
    elif endpoint == "status_changes":
        params["event_time"] = target_time_str # MDS 1.2 format for Bolt/Dott
    elif endpoint == "telemetry":
        params["telemetry_time"] = target_time_str # Specific to Voi
    elif "events" in endpoint: 
        # Handles Dott ('events') and Bolt/Voi ('events/recent')
        start_dt = now - timedelta(hours=2)
        params['start_time'] = int(start_dt.timestamp() * 1000)
        params['end_time'] = int(now.timestamp() * 1000)

    # --- Provider API Routing ---
    if provider == 'voi':
        token = get_voi_token()
        url = f"{Variable.get('VOI_MDS_URL')}/{Variable.get('VOI_ZONE_ID')}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.mds+json;version=2.0"}
        
    elif provider == 'dott':
        token = get_dott_token()
        url = f"https://mds.api.ridedott.com/{Variable.get('DOTT_REGION', 'brussels')}/{endpoint}"
        # Dott status_changes uses MDS 1.2, everything else is 2.0
        accept_version = "1.2" if endpoint == "status_changes" else "2.0"
        headers = {"Authorization": f"Bearer {token}", "Accept": f"application/vnd.mds.provider+json;version={accept_version}"}
        
    elif provider == 'bolt':
        token = get_bolt_token()
        url = f"https://mds.bolt.eu/{endpoint}"
        accept_version = "1.2" if endpoint == "status_changes" else "2.0"
        headers = {"Authorization": f"Bearer {token}", "Accept": f"application/vnd.mds+json;version={accept_version}"}
        
    elif provider == 'poppy':
        # Poppy uses a static API key and GBFS-style endpoints alongside MDS
        api_key = Variable.get("POPPY_API_KEY") 
        url = f"https://poppy.red/mds/{endpoint}"
        headers = {"external-api-key": api_key, "Accept": "application/vnd.mds+json;version=2.0"}

    # Execute Request
    response = requests.get(url, headers=headers, params=params, verify=False, timeout=30)
    
    # 404 Graceful Exit (Data not ready)
    if response.status_code == 404 and "trips" in endpoint: 
        return
        
    response.raise_for_status()
    data = response.json()

    # MD5 Deduplication
    content_str = json.dumps(data, sort_keys=True)
    current_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()
    pg_hook = PostgresHook(postgres_conn_id='postgres_raw')
    
    try:
        last_hash_record = pg_hook.get_first(f'SELECT md5_hash FROM "MICROMOBILITY_RAW"."{table_name}" ORDER BY file_ts DESC LIMIT 1')
        if last_hash_record and last_hash_record[0] == current_hash: 
            return # Skip insertion if payload hasn't changed
    except Exception: 
        pass 

    # Load to Postgres
    pg_hook.run(
        f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts, md5_hash) VALUES (%s, %s, %s, %s)',
        parameters=(content_str, f"{provider}_{endpoint.replace('/', '_')}_{target_time_str}.json", datetime.now(), current_hash)
    )

# --- 3. DAG STRUCTURE ---

PROVIDERS = ['voi', 'dott', 'bolt', 'poppy']

with DAG(
    'micromobility_unified_ingestion',
    start_date=datetime(2025, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    # 3.1 Shared Configuration
    project_cfg = ProjectConfig("/opt/airflow/voi_dbt")
    profile_cfg = ProfileConfig(
        profile_name="voi_mds", 
        target_name="prod",
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id="postgres_raw", 
            profile_args={"schema": "MICROMOBILITY_STAGING"} 
        )
    )

    # 3.2 Bronze Layer: Expanded Dictionary mapping to the Postman specs
    ENDPOINTS = {
        'voi': {
            'vehicles': 'VOI_VEHICLES', 
            'trips': 'VOI_TRIPS', 
            'vehicles/status': 'VOI_VEHICLES_STATUS',
            'events/recent': 'VOI_EVENTS',
            'telemetry': 'VOI_TELEMETRY'
        },
        'dott': {
            'vehicles': 'DOTT_VEHICLES', 
            'trips': 'DOTT_TRIPS', 
            'vehicles/status': 'DOTT_VEHICLES_STATUS',
            'events': 'DOTT_EVENTS',
            'status_changes': 'DOTT_STATUS_CHANGES'
        },
        'bolt': {
            'vehicles': 'BOLT_VEHICLES', 
            'trips': 'BOLT_TRIPS',
            'events/recent': 'BOLT_EVENTS', 
            'status_changes': 'BOLT_STATUS_CHANGES'
        },
        'poppy': {
            'trips/brussels': 'POPPY_TRIPS',
            'free_bike_status': 'POPPY_FREE_BIKE_STATUS',
            'vehicle_types': 'POPPY_VEHICLE_TYPES',
            'geofencing_zones': 'POPPY_GEOFENCING_ZONES',
            'system_information': 'POPPY_SYSTEM_INFORMATION',
            'system_pricing_plans': 'POPPY_SYSTEM_PRICING_PLANS'
        }
    }

    with TaskGroup("bronze_layer") as bronze_group:
        for provider, tasks in ENDPOINTS.items():
            for ep, table in tasks.items():
                PythonOperator(
                    task_id=f'fetch_{provider}_{ep.replace("/", "_")}',
                    python_callable=extract_and_load,
                    op_kwargs={'provider': provider, 'endpoint': ep, 'table_name': table}
                )

    # 3.3 Extraction Bridge
    extraction_bridge = EmptyOperator(
        task_id="extraction_bridge",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # 3.4 Silver Splinters
    provider_splinters = {}
    for provider in PROVIDERS:
        provider_splinters[provider] = DbtTaskGroup(
            group_id=f"silver_{provider}_transformation",
            project_config=project_cfg,
            profile_config=profile_cfg,
            render_config=RenderConfig(
                # ADDED THE ASTERISK (*): This fixes the 0 tasks bug
                select=[f"path:models/staging/stg_{provider}*"], 
                test_behavior=TestBehavior.AFTER_EACH
            )
        )

    # 3.5 Gold Hub
    gold_layer = DbtTaskGroup(
        group_id="gold_marts",
        project_config=project_cfg,
        profile_config=profile_cfg,
        render_config=RenderConfig(
            select=["path:models/marts"],
            test_behavior=TestBehavior.AFTER_ALL
        )
    )

    # 3.6 Final Dependency Flow
    bronze_group >> extraction_bridge
    for provider in PROVIDERS:
        extraction_bridge >> provider_splinters[provider] >> gold_layer