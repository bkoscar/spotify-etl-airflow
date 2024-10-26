import os
import requests
import base64
import pandas as pd
import pytz
import logging
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
from typing import Optional, Dict

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuthToken:
    """Handles Spotify API authentication and token management."""

    def __init__(self):
        """Initializes the AuthToken class and loads environment variables."""
        env_path = os.path.join(os.path.dirname(__file__), '../config/.env')
        load_dotenv(dotenv_path=env_path)
        self._client_id = os.getenv('SPOTIFY_CLIENT_ID')
        self._client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        self._refresh_token = os.getenv('REFRESH_TOKEN')
        self._uri_api_token = os.getenv('URL_API_TOKEN')
        self._spotify_uri = os.getenv('SPOTIFY_URI')

        if not all([self._client_id, self._client_secret, self._refresh_token, self._uri_api_token, self._spotify_uri]):
            logger.error("Missing one or more environment variables")
            raise ValueError("Missing one or more environment variables")

        # Encode the client ID and client secret
        self._encoded = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        self._token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None

    def _get_refresh_token(self) -> Optional[str]:
        """Fetches a new access token using the refresh token.

        Returns:
            Optional[str]: The new access token, or None if an error occurs.
        """
        body = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self._encoded}"
        }
        try:
            response = requests.post(self._uri_api_token, data=body, headers=headers)
            response.raise_for_status()
            req = response.json()
            self._token = req['access_token']
            self._token_expiry = datetime.now() + timedelta(seconds=req['expires_in'])
            return self._token
        except requests.exceptions.RequestException as e:
            logger.exception("Error fetching refresh token")
            return None


class ExtractData(AuthToken):
    """Handles the extraction of data from Spotify."""

    def get_data(self) -> Dict:
        """Retrieves recently played tracks from Spotify.

        Returns:
            Dict: The raw data of recently played tracks.
        """
        if self._token is None or datetime.now() >= self._token_expiry:
            self._get_refresh_token()
        if not self._token:
            return {}

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._token}"
        }

        # Calculate the Unix timestamp (in milliseconds) for today's midnight
        now = datetime.now()
        midnight_today = datetime(now.year, now.month, now.day)
        midnight_unix_timestamp = int(midnight_today.timestamp()) * 1000

        try:
            response = requests.get(
                f"https://api.spotify.com/v1/me/player/recently-played?after={midnight_unix_timestamp}&limit=50",
                headers=headers)
            response.raise_for_status()
            raw_data = response.json()
            return raw_data
        except requests.exceptions.RequestException as e:
            logger.exception("Error fetching data")
            return {}


class TransformData:
    """Transforms raw Spotify data into a structured format suitable for loading into a database."""

    def __init__(self):
        """Initializes the TransformData class by extracting raw data."""
        self._raw_data = ExtractData().get_data()

    def save_data_df(self) -> pd.DataFrame:
        """Processes the raw data and transforms it into a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the transformed data.
        """
        if len(self._raw_data.get('items', [])) == 0:
            logger.info("No played music today.")
            return pd.DataFrame()  # Return an empty DataFrame
        else:
            data = []
            for r in self._raw_data['items']:
                try:
                    played_at_utc = datetime.strptime(r["played_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    played_at_local = played_at_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Mexico_City'))
                    data.append({
                        "played_at_id": r["played_at"],
                        "artist_name": r["track"]["artists"][0]["name"],
                        "song_name": r["track"]["name"],
                        "popularity": r["track"]["popularity"],
                        "played_at_local": played_at_local.strftime("%Y-%m-%d %H:%M:%S")
                    })
                except (KeyError, ValueError) as e:
                    logger.exception(f"Error processing record: {r}")
            song_df = pd.DataFrame(data)
            return song_df

    def data_validation_primary_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validates that the 'played_at_id' column in the DataFrame is unique.

        Args:
            df (pd.DataFrame): The DataFrame to validate.

        Raises:
            Exception: If 'played_at_id' is not unique.

        Returns:
            pd.DataFrame: The validated DataFrame.
        """
        if df.empty:
            logger.info("No data to validate.")
            return df
        df = df.sort_values(by=['played_at_id'])
        if not df['played_at_id'].is_unique:
            logger.error("Primary Key check is violated")
            raise Exception("Primary Key check is violated")
        return df

    def transform_data(self) -> pd.DataFrame:
        """Transforms and validates the data.

        Returns:
            pd.DataFrame: The transformed and validated DataFrame ready for loading.
        """
        data = self.save_data_df()
        if data.empty:
            return data
        data = self.data_validation_primary_key(data)
        return data


class Load:
    """Manages loading data into a PostgreSQL database using Airflow's PostgresHook."""

    def __init__(self):
        """Initializes the Load class with the PostgreSQL connection ID."""
        # Ensure 'POSTGRES_CONN_ID' is set in your Airflow connections
        self.postgres_conn_id = os.getenv('POSTGRES_CONN_ID', 'postgres')

    def load_data(self, df: pd.DataFrame, table_name: str):
        """Loads the transformed data into a PostgreSQL table, handling duplicates.

        Args:
            df (pd.DataFrame): The DataFrame containing the transformed data.
            table_name (str): The target table name in PostgreSQL.
        """
        if df.empty:
            logger.info("No data to load into the database.")
            return
        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # Convert the DataFrame to a list of tuples
        data = [tuple(row) for row in df.to_numpy()]
        columns = list(df.columns)
        # Build the INSERT query with ON CONFLICT DO NOTHING
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            # Use execute_values for efficient bulk insertion
            from psycopg2.extras import execute_values
            with postgres_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    execute_values(cursor, insert_sql, data)
                    conn.commit()
            logger.info(f"Data successfully loaded into the table {table_name}.")
        except Exception as e:
            logger.exception("Error loading data into the database")


class ETL:
    """Orchestrates the full ETL process: Extract, Transform, Load."""

    def __init__(self):
        """Initializes the ETL process components."""
        self.transformer = TransformData()
        self.loader = Load()

    def run(self):
        """Runs the full ETL pipeline."""
        logger.info("Starting ETL process...")
        transformed_data = self.transformer.transform_data()
        if transformed_data.empty:
            logger.info("No data to load. ETL process completed successfully.")
        else:
            logger.info("Data extracted and transformed successfully.")
            self.loader.load_data(transformed_data, "spotify_table")
            logger.info("ETL process completed.")


def etl_process():
    """Entry point for running the ETL process."""
    etl = ETL()
    etl.run()
