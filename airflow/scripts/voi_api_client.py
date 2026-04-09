import os
import sys
import requests
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

def load_env_universally():
    """
    Searches for the .env file starting from the script's directory 
    and moving upwards through parent directories.
    """
    current_path = Path(__file__).resolve().parent
    # Check current directory and all parents
    for path in [current_path] + list(current_path.parents):
        env_file = path / ".env"
        if env_file.exists():
            load_dotenv(dotenv_path=env_file, override=True)
            return env_file
    return None

# --- Environment Setup ---
env_path = load_env_universally()

if env_path:
    print(f"Environment: Successfully loaded .env from {env_path}")
else:
    print("Critical Error: .env file not found in any parent directories.")
    sys.exit(1)

# --- Configuration Mapping ---
VOI_USER_ID = os.getenv("VOI_USER_ID")
VOI_PASSWORD = os.getenv("VOI_PASSWORD")
VOI_AUTH_URL = os.getenv("VOI_AUTH_URL")
VOI_MDS_URL = os.getenv("VOI_MDS_URL")
VOI_ZONE_ID = os.getenv("VOI_ZONE_ID")

RAW_HOST = os.getenv("PG_HOST")
DB_PORT = os.getenv("PG_PORT", "5432")
DB_NAME = os.getenv("PG_DATABASE", "stage_micromobility")
DB_USER = os.getenv("PG_USER", "tan")
DB_PASS = os.getenv("PG_PASS")

# Logic: Switch host to localhost if running manually outside of Docker
if RAW_HOST == "host.docker.internal":
    DB_HOST = "localhost"
else:
    DB_HOST = RAW_HOST

class VoiApiClient:
    def __init__(self):
        self.token = None
        self.headers = {
            "Accept": "application/vnd.mds+json;version=2.0"
        }

    def get_token(self):
        """Perform OAuth 2.0 Client Credentials Grant."""
        print(f"Authenticating with VOI at {VOI_AUTH_URL}...")
        try:
            response = requests.post(
                VOI_AUTH_URL,
                auth=(VOI_USER_ID, VOI_PASSWORD),
                data={'grant_type': 'client_credentials'}
            )
            response.raise_for_status()
            self.token = response.json().get("access_token")
            self.headers["Authorization"] = f"Bearer {self.token}"
            print("Authentication: Token acquired successfully.")
            return self.token
        except Exception as e:
            print(f"Authentication Failed: {e}")
            raise

    def fetch_endpoint_data(self, endpoint, params=None):
        """Fetch data from a specific MDS endpoint."""
        if not self.token:
            self.get_token()

        url = f"{VOI_MDS_URL}/{VOI_ZONE_ID}/{endpoint}"
        print(f"API Request: GET {url}")
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API Error fetching {endpoint}: {e}")
            return None

    def load_to_postgres(self, table_name, content, filename, file_ts):
        """Insert raw JSON content and metadata into PostgreSQL."""
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            cur = conn.cursor()

            query = f'''
                INSERT INTO "MICROMOBILITY_RAW"."{table_name}" 
                (content, filename, file_ts) 
                VALUES (%s, %s, %s)
            '''
            
            cur.execute(query, (json.dumps(content), filename, file_ts))
            conn.commit()
            print(f"Database: Successfully loaded data into {table_name}.")

        except Exception as e:
            print(f"Database Error: {e}")
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    client = VoiApiClient()
    
    print(f"--- STARTING DATA EXTRACTION ---")
    print(f"Target Host: {DB_HOST} | Zone ID: {VOI_ZONE_ID}")

    # Fetching Vehicles
    vehicles_data = client.fetch_endpoint_data("vehicles")
    if vehicles_data:
        client.load_to_postgres(
            table_name="VOI_VEHICLES",
            content=vehicles_data,
            filename=f"voi_vehicles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            file_ts=datetime.now()
        )

    print(f"--- EXTRACTION FINISHED ---")