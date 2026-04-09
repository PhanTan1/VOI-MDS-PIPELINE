import json
import requests
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowRescheduleException
from airflow.sdk import TaskGroup

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# --- Dynamic Configuration ---
def get_provider_configs(provider):
    """Dynamically fetch variables based on the provider name (e.g., VOI, POPPY)."""
    p = provider.upper()
    return {
        "ZONE_ID": Variable.get(f"{p}_ZONE_ID"),
        "AUTH_URL": Variable.get(f"{p}_AUTH_URL"),
        "MDS_URL": Variable.get(f"{p}_MDS_URL"),
        "USER_ID": Variable.get(f"{p}_USER_ID"),
        "PASSWORD": Variable.get(f"{p}_PASSWORD")
    }

# --- Helper Functions ---
def get_auth_token(configs):
    """Exchange credentials for an OAuth2 Bearer token."""
    response = requests.post(
        configs["AUTH_URL"],
        auth=(configs["USER_ID"], configs["PASSWORD"]),
        data={'grant_type': 'client_credentials'}
    )
    response.raise_for_status()
    return response.json().get("access_token")

def extract_and_load(endpoint, table_name, provider, **kwargs):
    """Generic function to fetch data and load it into PostgreSQL."""
    configs = get_provider_configs(provider)
    token = get_auth_token(configs)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.mds+json;version=2.0"
    }

    # MDS 2.0 targets the end of the data interval
    stabilized_window = kwargs['data_interval_start'] - timedelta(hours=1)
    target_time = stabilized_window.strftime("%Y-%m-%dT%H")
    
    params = {"end_time": target_time} if endpoint == "trips" else \
             {"event_time": target_time} if endpoint == "events/historical" else {}

    url = f"{configs['MDS_URL']}/{configs['ZONE_ID']}/{endpoint}"
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 404:
        raise AirflowRescheduleException(reschedule_date=pendulum.now('UTC').add(minutes=10))
    
    data = response.json()
    filename = f"{provider}_{endpoint.replace('/', '_')}_{target_time}.json"

    pg_hook = PostgresHook(postgres_conn_id='postgres_raw')
    query = f'INSERT INTO "MICROMOBILITY_RAW"."{table_name}" (content, filename, file_ts) VALUES (%s, %s, %s)'
    pg_hook.run(query, parameters=(json.dumps(data), filename, kwargs['data_interval_end']))

# --- DAG Definition ---
PROVIDERS = ['voi']  # To add Poppy, just change this to ['voi', 'poppy']

with DAG(
    'mds_ingestion_hourly',
    default_args={'owner': 'tphan', 'retries': 1, 'retry_delay': timedelta(minutes=5)},
    schedule='30 * * * *',
    start_date=datetime(2026, 3, 30),
    catchup=False,
    tags=['mobility', 'raw', 'dbt']
) as dag:

    with TaskGroup(group_id="raw_ingestion") as ingestion_group:
        for provider in PROVIDERS:
            with TaskGroup(group_id=f"{provider}_ingestion") as p_group:
                
                # Define endpoints and their target tables
                endpoints = {
                    'vehicles': f'{provider.upper()}_VEHICLES',
                    'vehicles/status': f'{provider.upper()}_VEHICLES_STATUS',
                    'trips': f'{provider.upper()}_TRIPS',
                    'events/historical': f'{provider.upper()}_EVENTS'
                }

                # 1. Registry must run first for referential integrity
                registry_task = PythonOperator(
                    task_id=f'fetch_{provider}_vehicles',
                    python_callable=extract_and_load,
                    op_kwargs={'endpoint': 'vehicles', 'table_name': endpoints.pop('vehicles'), 'provider': provider}
                )

                # 2. Remaining endpoints run in parallel
                other_tasks = [
                    PythonOperator(
                        task_id=f'fetch_{provider}_{ep.replace("/", "_")}',
                        python_callable=extract_and_load,
                        op_kwargs={'endpoint': ep, 'table_name': tbl, 'provider': provider}
                    ) for ep, tbl in endpoints.items()
                ]

                registry_task >> other_tasks

    # --- DBT INTEGRATION ---
    dbt_group = DbtTaskGroup(
        group_id="transformation",
        project_config=ProjectConfig("/opt/airflow/voi_dbt"),
        profile_config=ProfileConfig(
            profile_name="voi_mds",
            target_name="prod",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_raw",
                profile_args={"schema": "MICROMOBILITY_STAGING"},
            ),
        ),
        execution_config=ExecutionConfig(dbt_executable_path="/home/airflow/.local/bin/dbt"),
    )

    ingestion_group >> dbt_group