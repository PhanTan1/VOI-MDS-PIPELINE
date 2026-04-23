import sys
import os
from datetime import datetime
from airflow import DAG
try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# Add dags folder to path so utils can be found
sys.path.append(os.path.dirname(__file__))

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior
from cosmos.constants import LoadMode

# Import the engine
from utils.api_ingestion import extract_and_load

# --- 1. CONFIGURATION ---
project_cfg = ProjectConfig(
    dbt_project_path="/opt/airflow/voi_dbt",
    manifest_path="/opt/airflow/voi_dbt/target/manifest.json" # Ensure this path is correct
)
profile_cfg = ProfileConfig(
    profile_name="mds", 
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_raw", 
        profile_args={"schema": "MICROMOBILITY_STAGING"} 
    )
)

PROVIDERS = ['voi', 'dott', 'bolt', 'poppy']

HOURLY_ENDPOINTS = {
    'voi': {
        'vehicles': 'VOI_VEHICLES', 
        'trips': 'VOI_TRIPS', 
        'vehicles/status': 'VOI_VEHICLES_STATUS', 
        'events/historical': 'VOI_EVENTS'
    },
    'dott': {
        'vehicles': 'DOTT_VEHICLES', 
        'trips': 'DOTT_TRIPS', 
        'vehicles/status': 'DOTT_VEHICLES_STATUS', 
        'events': 'DOTT_EVENTS', 
        'telemetry': 'DOTT_TELEMETRY'
    },
    'bolt': {
        'vehicles': 'BOLT_VEHICLES', 
        'trips': 'BOLT_TRIPS', 
        'events/historical': 'BOLT_EVENTS', 
        'status_changes': 'BOLT_STATUS_CHANGES', # MDS 1.2
        'telemetry': 'BOLT_TELEMETRY'           # MDS 2.0 - Added for Routing logic
    },
    'poppy': {
        'free_bike_status': 'POPPY_FREE_BIKE_STATUS'
    }
}

DAILY_ENDPOINTS = {
    'poppy': {
        'trips/brussels': 'POPPY_TRIPS', 
        'vehicle_types': 'POPPY_VEHICLE_TYPES', 
        'system_information': 'POPPY_SYSTEM_INFORMATION'
    }
}

# --- 2. DAGS ---
with DAG('micromobility_hourly_ingestion', start_date=datetime(2025, 1, 1), schedule='@hourly', catchup=False) as dag_hourly:
    
    bronze_lanes = {}
    silver_lanes = {}

    # 1. BRONZE LAYER: Parallel Extraction
    # Automatically creates tasks for the new Bolt telemetry and status_changes endpoints
    with TaskGroup("bronze_layer") as bronze:
        for provider in PROVIDERS:
            with TaskGroup(group_id=provider) as provider_bronze:
                for ep, table in HOURLY_ENDPOINTS[provider].items():
                    PythonOperator(
                        task_id=f'fetch_{ep.replace("/", "_")}',
                        python_callable=extract_and_load,
                        op_kwargs={'provider': provider, 'endpoint': ep, 'table_name': table}
                    )
            bronze_lanes[provider] = provider_bronze

    # 2. SILVER LAYER: Parallel Transformation
    # Cosmos will automatically include stg_bolt_telemetry.sql and stg_bolt_status_changes.sql
    with TaskGroup("silver_layer") as silver:
        for provider in PROVIDERS:
            folder_name = provider.capitalize() 
            
            silver_lanes[provider] = DbtTaskGroup(
                group_id=f"stg_{provider}",
                project_config=project_cfg,
                profile_config=profile_cfg,
                render_config=RenderConfig(
                    select=[f"path:models/staging/{folder_name}"],
                    load_method=LoadMode.DBT_MANIFEST,
                    emit_upstream_tasks=True, 
                    select=['path:models', 'path:seeds'],
                )
            )

    # 3. GOLD LAYER: Merged Analytics
    gold = DbtTaskGroup(
        group_id="gold_marts",
        project_config=project_cfg,
        profile_config=profile_cfg,
        render_config=RenderConfig(
            select=["path:models/marts"],
            load_method=LoadMode.DBT_MANIFEST
        )
    )

    # --- UPDATED DEPENDENCY FLOW ---
    for provider in PROVIDERS:
        bronze_lanes[provider] >> silver_lanes[provider]
        silver_lanes[provider] >> gold

with DAG('micromobility_daily_batch', start_date=datetime(2025, 1, 1), schedule='0 3 * * *', catchup=False) as dag_daily:
    for ep, table in DAILY_ENDPOINTS['poppy'].items():
        PythonOperator(
            task_id=f'fetch_daily_{ep.replace("/", "_")}',
            python_callable=extract_and_load,
            op_kwargs={'provider': 'poppy', 'endpoint': ep, 'table_name': table}
        )