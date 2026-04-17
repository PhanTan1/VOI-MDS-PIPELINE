import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# Add dags folder to path so utils can be found
sys.path.append(os.path.dirname(__file__))

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

# Import the engine
from utils.api_ingestion import extract_and_load

# --- 1. CONFIGURATION ---
project_cfg = ProjectConfig("/opt/airflow/voi_dbt")
profile_cfg = ProfileConfig(
    profile_name="voi_mds", 
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_raw", 
        profile_args={"schema": "MICROMOBILITY_STAGING"} 
    )
)

PROVIDERS = ['voi', 'dott', 'bolt', 'poppy']

HOURLY_ENDPOINTS = {
    'voi': {'vehicles': 'VOI_VEHICLES', 'trips': 'VOI_TRIPS', 'vehicles/status': 'VOI_VEHICLES_STATUS', 'events/historical': 'VOI_EVENTS'},
    'dott': {'vehicles': 'DOTT_VEHICLES', 'trips': 'DOTT_TRIPS', 'vehicles/status': 'DOTT_VEHICLES_STATUS', 'events/historical': 'DOTT_EVENTS', 'telemetry': 'DOTT_TELEMETRY'},
    'bolt': {'vehicles': 'BOLT_VEHICLES', 'trips': 'BOLT_TRIPS', 'events/historical': 'BOLT_EVENTS', 'status_changes': 'BOLT_STATUS_CHANGES'},
    'poppy': {'free_bike_status': 'POPPY_FREE_BIKE_STATUS'}
}

DAILY_ENDPOINTS = {
    'poppy': {'trips/brussels': 'POPPY_TRIPS', 'vehicle_types': 'POPPY_VEHICLE_TYPES', 'system_information': 'POPPY_SYSTEM_INFORMATION'}
}

# --- 2. DAGS ---
with DAG('micromobility_hourly_ingestion', start_date=datetime(2025, 1, 1), schedule='@hourly', catchup=False) as dag_hourly:
    with TaskGroup("bronze_layer") as bronze:
        for provider, tasks in HOURLY_ENDPOINTS.items():
            for ep, table in tasks.items():
                PythonOperator(
                    task_id=f'fetch_{provider}_{ep.replace("/", "_")}',
                    python_callable=extract_and_load,
                    op_kwargs={'provider': provider, 'endpoint': ep, 'table_name': table}
                )

    bridge = EmptyOperator(task_id="extraction_bridge", trigger_rule=TriggerRule.ALL_DONE)
    
    gold = DbtTaskGroup(
        group_id="gold_marts",
        project_config=project_cfg,
        profile_config=profile_cfg,
        render_config=RenderConfig(select=["path:models/marts"])
    )

    bronze >> bridge >> gold

with DAG('micromobility_daily_batch', start_date=datetime(2025, 1, 1), schedule='0 3 * * *', catchup=False) as dag_daily:
    for ep, table in DAILY_ENDPOINTS['poppy'].items():
        PythonOperator(
            task_id=f'fetch_daily_{ep.replace("/", "_")}',
            python_callable=extract_and_load,
            op_kwargs={'provider': 'poppy', 'endpoint': ep, 'table_name': table}
        )