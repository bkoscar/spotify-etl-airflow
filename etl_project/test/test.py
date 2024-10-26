import os
import requests
import base64
import pandas as pd
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta



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
            raise ValueError("Missing one or more environment variables")

        # Encode the client ID and client secret
        self._encoded = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        self._token = None
        self._token_expiry = None

    def _get_refresh_token(self):
        """Fetches a new access token using the refresh token.

        Returns:
            str: The new access token, or None if an error occurs.
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
            print(f"Error fetching refresh token: {e}")
            return None


class ExtractData(AuthToken):
    """Handles the extraction of data from Spotify."""

    def get_data(self):
        """Retrieves recently played tracks from Spotify.

        Returns:
            dict: The raw data of recently played tracks.
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
            print(f"Error fetching data: {e}")
            return {}


class TransformData:
    """Transforms raw Spotify data into a structured format suitable for loading into a database."""

    def __init__(self):
        """Initializes the TransformData class by extracting raw data."""
        self._raw_data = ExtractData().get_data()

    def save_data_df(self):
        """Processes the raw data and transforms it into a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing the transformed data.
        """
        if len(self._raw_data.get('items', [])) == 0:
            print("No played music today.")
            return pd.DataFrame()  # Return an empty DataFrame
        else:
            data = []
            for r in self._raw_data['items']:
                played_at_utc = datetime.strptime(r["played_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                played_at_local = played_at_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Mexico_City'))
                data.append({
                    "played_at_id": r["played_at"],
                    "artist_name": r["track"]["artists"][0]["name"],
                    "song_name": r["track"]["name"],
                    "popularity": r["track"]["popularity"],
                    "played_at_local": played_at_local.strftime("%Y-%m-%d %H:%M:%S")
                })
            song_df = pd.DataFrame(data)
        return song_df

    def data_validation_primary_key(self, df: pd.DataFrame):
        """Validates that the 'played_at_id' column in the DataFrame is unique.

        Args:
            df (pd.DataFrame): The DataFrame to validate.

        Raises:
            Exception: If 'played_at_id' is not unique.

        Returns:
            pd.DataFrame: The validated DataFrame.
        """
        if df.empty:
            print("No data to validate.")
            return df
        df = df.sort_values(by=['played_at_id'])
        if pd.Series(df['played_at_id']).is_unique:
            pass
        else:
            raise Exception("Primary Key check is violated")
        return df

    def transform_data(self):
        """Transforms and validates the data.

        Returns:
            pd.DataFrame: The transformed and validated DataFrame ready for loading.
        """
        data = self.save_data_df()
        if data.empty:
            return data
        data = self.data_validation_primary_key(data)
        return data


class ETL:
    """Orchestrates the full ETL process: Extract, Transform, Load."""

    def __init__(self):
        """Initializes the ETL process components."""
        self.extractor = ExtractData()
        self.transformer = TransformData()

    def run(self):
        """Runs the full ETL pipeline.

        This method coordinates the extraction of data from the Spotify API, the transformation
        of that data into a suitable format, and the loading of the data into a PostgreSQL database.
        """
        print("Starting ETL process...")
        transformed_data = self.transformer.transform_data()
        print(transformed_data)
        if transformed_data.empty:
            print("No data to load. ETL process completed successfully.")
        else:
            print("Data extracted and transformed successfully.")
            print("ETL process completed.")


def etl_process():
    """Entry point for running the ETL process."""
    etl = ETL()
    etl.run()

if __name__ == '__main__':
    etl_process()
