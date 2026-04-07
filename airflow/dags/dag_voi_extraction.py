import os
import json
import requests
import psycopg2
import pendulum
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowRescheduleException
from airflow.sdk import TaskGroup

# --- Environment Loading ---
def load_env_universally():
    """Finds and loads the .env file from the project root."""
    current_path = Path(__file__).resolve().parent
    for path in [current_path] + list(current_path.parents):
        env_file = path / ".env"
        if env_file.exists():
            load_dotenv(dotenv_path=env_file, override=True)
            return env_file
    return None

load_env_universally()

# Configuration
VOI_ZONE_ID = os.getenv("VOI_ZONE_ID")
VOI_AUTH_URL = os.getenv("VOI_AUTH_URL")
VOI_MDS_URL = os.getenv("VOI_MDS_URL")
PG_HOST = os.getenv("PG_HOST") 
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

# --- Helper Functions ---

def get_voi_token():
    """Exchange credentials for an OAuth2 Bearer token."""
    response = requests.post(
        VOI_AUTH_URL,
        auth=(os.getenv("VOI_USER_ID"), os.getenv("VOI_PASSWORD")),
        data={'grant_type': 'client_credentials'}
    )
    response.raise_for_status()
    return response.json().get("access_token")

def extract_and_load(endpoint, table_name, **kwargs):
    """
    Generic function to fetch data and load it into PostgreSQL.
    Handles 404 by rescheduling the task for later.
    """
    token = get_voi_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.mds+json;version=2.0"
    }

    # MDS 2.0 targets the end of the data interval
    stabilized_window = kwargs['data_interval_start'] - timedelta(hours=1)
    target_time = stabilized_window.strftime("%Y-%m-%dT%H")
    
    print(f"Requesting stabilized data for hour: {target_time}")

    params = {}
    if endpoint == "trips":
        params = {"end_time": target_time}
    elif endpoint == "events/historical":
        params = {"event_time": target_time}

    url = f"{VOI_MDS_URL}/{VOI_ZONE_ID}/{endpoint}"
    response = requests.get(url, headers=headers, params=params)

    # Handle the 404 Case: Data is not ready yet
    if response.status_code == 404:
        print(f"Data for {target_time} not found. VOI might still be processing. Rescheduling...")      
        reschedule_time = pendulum.now('UTC').add(minutes=10)
        raise AirflowRescheduleException(reschedule_date=reschedule_time)
    
    data = response.json()

    # Database Loading
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()

    filename = f"voi_{endpoint.replace('/', '_')}_{target_time}.json"
    
    query = f'''
        INSERT INTO "PROD_MICROMOBILITY_RAW"."{table_name}" 
        (content, filename, file_ts) 
        VALUES (%s, %s, %s)
    '''
    
    cur.execute(query, (json.dumps(data), filename, kwargs['data_interval_end']))
    conn.commit()
    
    cur.close()
    conn.close()
    print(f"Successfully loaded {endpoint} data into {table_name}")

# --- Common dbt Environment ---
dbt_env = {
    **os.environ,
    'PG_HOST': PG_HOST,
    'PG_USER': PG_USER,
    'PG_PASS': PG_PASS,
    'PG_PORT': PG_PORT,
    'PG_DATABASE': PG_DATABASE,
    'DBT_PROFILES_DIR': '/opt/airflow/voi_dbt'
}

# --- DAG Definition ---

default_args = {
    'owner': 'tphan',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'voi_mds_ingestion_hourly',
    default_args=default_args,
    description='Extract VOI MDS 2.0 data and load to External Postgres RAW layer',
    schedule='30 * * * *',
    start_date=datetime(2026, 3, 30),
    catchup=False,
    tags=['voi', 'mds', 'raw', 'dbt']
) as dag:

    with TaskGroup("voi_ingestion") as ingestion_group:
        
        # 1. Registry must be extracted first
        task_fetch_vehicles = PythonOperator(
            task_id='fetch_voi_vehicles',
            python_callable=extract_and_load,
            op_kwargs={'endpoint': 'vehicles', 'table_name': 'VOI_VEHICLES'}
        )

        # 2. Status, Trips, and Events can run in parallel afterward
        task_fetch_status = PythonOperator(
            task_id='fetch_voi_status',
            python_callable=extract_and_load,
            op_kwargs={'endpoint': 'vehicles/status', 'table_name': 'VOI_VEHICLES_STATUS'}
        )

        task_fetch_trips = PythonOperator(
            task_id='fetch_voi_trips',
            python_callable=extract_and_load,
            op_kwargs={'endpoint': 'trips', 'table_name': 'VOI_TRIPS'}
        )

        task_fetch_events = PythonOperator(
            task_id='fetch_voi_events',
            python_callable=extract_and_load,
            op_kwargs={'endpoint': 'events/historical', 'table_name': 'VOI_EVENTS'}
        )

        # Registry enforces referential integrity for the others
        task_fetch_vehicles >> [task_fetch_status, task_fetch_trips, task_fetch_events]

    with TaskGroup("voi_transformation") as dbt_group:
        
        # 1. Transform the registry first so the master UUID mapping is updated
        dbt_run_registry = BashOperator(
            task_id="dbt_stg_registry",
            bash_command='/home/airflow/.local/bin/dbt run --select stg_voi_vehicles',
            cwd='/opt/airflow/voi_dbt',
            env=dbt_env
        )

        # 2. Run the remaining staging models AND the final marts.
        # Using 'fct_trips_vianova' (the filename) instead of 'f_trip' (the alias)
        dbt_run_marts = BashOperator(
            task_id="dbt_marts_status_trips",
            bash_command='/home/airflow/.local/bin/dbt run --select stg_voi_vehicles_status stg_voi_trips fct_vehicles_status fct_trips_vianova',
            cwd='/opt/airflow/voi_dbt',
            env=dbt_env
        )

        dbt_run_registry >> dbt_run_marts

    # --- Pipeline Execution Flow ---
    ingestion_group >> dbt_group