import os
import json
import requests
import psycopg2
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator

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
PG_HOST = os.getenv("PG_HOST") # Inside Docker, this remains 'host.docker.internal'
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
    kwargs['data_interval_end'] provides the current execution hour for MDS parameters.
    """
    token = get_voi_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.mds+json;version=2.0"
    }

    # Determine time parameters for /trips or /events
    # MDS 2.0 expects YYYY-MM-DDTHH
    target_time = kwargs['data_interval_end'].strftime("%Y-%m-%dT%H")
    params = {}
    
    if endpoint == "trips":
        params = {"end_time": target_time}
    elif endpoint == "events/historical":
        params = {"event_time": target_time}

    url = f"{VOI_MDS_URL}/{VOI_ZONE_ID}/{endpoint}"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
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
    description='Extract VOI MDS 2.0 data and load to Local Postgres RAW layer',
    schedule='@hourly',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags=['voi', 'mds', 'raw']
) as dag:

    task_fetch_vehicles = PythonOperator(
        task_id='fetch_voi_vehicles',
        python_callable=extract_and_load,
        op_kwargs={'endpoint': 'vehicles', 'table_name': 'VOI_VEHICLES'}
    )

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

    # Parallel execution
    [task_fetch_vehicles, task_fetch_status, task_fetch_trips, task_fetch_events]