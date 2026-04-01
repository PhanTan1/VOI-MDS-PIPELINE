import os
import requests
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# --- Environment Setup ---
project_root = Path(__file__).resolve().parent.parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path)

# VOI Config
VOI_USER_ID = os.getenv("VOI_USER_ID")
VOI_PASSWORD = os.getenv("VOI_PASSWORD")
VOI_AUTH_URL = os.getenv("VOI_AUTH_URL")
VOI_MDS_URL = os.getenv("VOI_MDS_URL")
VOI_ZONE_ID = os.getenv("VOI_ZONE_ID")

# Postgres Config
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DATABASE", "stage_micromobility")
DB_USER = os.getenv("PG_USER", "tan")
DB_PASS = os.getenv("PG_PASS", "password")

class VoiApiClient:
    def __init__(self):
        self.token = None
        self.headers = {
            "Accept": "application/vnd.mds+json;version=2.0"
        }

    def get_token(self):
        """Perform OAuth 2.0 Client Credentials Grant to get a Bearer token."""
        print("Authenticating with VOI API...")
        try:
            response = requests.post(
                VOI_AUTH_URL,
                auth=(VOI_USER_ID, VOI_PASSWORD),
                data={'grant_type': 'client_credentials'}
            )
            response.raise_for_status()
            self.token = response.json().get("access_token")
            self.headers["Authorization"] = f"Bearer {self.token}"
            print("Authentication successful.")
            return self.token
        except Exception as e:
            print(f"Authentication failed: {e}")
            raise

    def fetch_endpoint_data(self, endpoint, params=None):
        """Generic method to fetch data from a specific MDS endpoint."""
        if not self.token:
            self.get_token()

        url = f"{VOI_MDS_URL}/{VOI_ZONE_ID}/{endpoint}"
        print(f"Fetching data from: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching {endpoint}: {e}")
            return None

    def load_to_postgres(self, table_name, content, filename, file_ts):
        """Insert raw JSON content and metadata into the RAW schema."""
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            cur = conn.cursor()

            # The schema name is forced to uppercase via double quotes
            query = f'''
                INSERT INTO "PROD_MICROMOBILITY_RAW"."{table_name}" 
                (content, filename, file_ts) 
                VALUES (%s, %s, %s)
            '''
            
            cur.execute(query, (json.dumps(content), filename, file_ts))
            conn.commit()
            print(f"Successfully loaded data into {table_name}.")

        except Exception as e:
            print(f"Database load error: {e}")
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()

# --- Execution Logic (for manual testing) ---
if __name__ == "__main__":
    client = VoiApiClient()
    
    # Example 1: Fetching Vehicles (Inventory)
    # This endpoint typically doesn't require time parameters
    vehicles_data = client.fetch_endpoint_data("vehicles")
    if vehicles_data:
        client.load_to_postgres(
            table_name="VOI_VEHICLES",
            content=vehicles_data,
            filename=f"voi_vehicles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            file_ts=datetime.now()
        )

    # Example 2: Fetching Trips for a specific hour
    # MDS 2.0 /trips requires end_time in YYYY-MM-DDTHH format
    target_hour = "2024-06-13T10" 
    trips_data = client.fetch_endpoint_data("trips", params={"end_time": target_hour})
    if trips_data:
        client.load_to_postgres(
            table_name="VOI_TRIPS",
            content=trips_data,
            filename=f"voi_trips_{target_hour.replace(':', '')}.json",
            file_ts=datetime.strptime(target_hour, "%Y-%m-%dT%H")
        )