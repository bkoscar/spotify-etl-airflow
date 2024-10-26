from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pendulum

# Define default_args
default_args = {
    'owner': 'Create Table DAG',
    'start_date': pendulum.datetime(2024, 10, 24, 23, 15, tz="America/Mexico_City"),  # Set to a future date and time
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="create_spotify_table",
    default_args=default_args,
    description="A DAG for creating the Spotify table in PostgreSQL",
    schedule_interval=None,  # Manually triggered
    catchup=False,  # Prevents Airflow from running past tasks if the DAG is inactive for a while
    max_active_runs=1,  # Ensures only one instance of the DAG runs at a time
    tags=["spotify", "create_table"],
) as dag:
    
    # Define the task to create the Spotify table in PostgreSQL
    create_table = PostgresOperator(
        task_id="create_table",  # Unique identifier for the task
        postgres_conn_id="postgres",  # Connection ID for the PostgreSQL database
        sql="""
        CREATE TABLE IF NOT EXISTS spotify_table (
            played_at_id VARCHAR(200),  # Primary key for the table
            artist_name VARCHAR(200),  # Name of the artist
            song_name VARCHAR(200),  # Name of the song
            popularity VARCHAR(200),  # Popularity of the song
            played_at_local TIMESTAMP,  # Timestamp when the song was played
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at_id)  # Primary key constraint
        )
        """,
    )

    # Set the task sequence
    create_table