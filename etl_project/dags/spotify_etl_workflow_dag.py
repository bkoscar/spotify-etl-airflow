from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import pendulum
from etl_process import etl_process

# Define default_args
default_args = {
    'owner': 'Insert Table DAG',
    'start_date': pendulum.datetime(2023, 10, 24, 23, 0, tz="America/Mexico_City"),  
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="spotify_etl_dag",
    default_args=default_args,
    description="A DAG for running the Spotify ETL process",
    schedule_interval='58 11 * * *',  # Runs at 11:58 PM every day
    catchup=False,  
    max_active_runs=1,  
    tags=["spotify", "etl"],
) as dag:

    # Define the ETL task
    run_etl = PythonOperator(
        task_id="run_etl_process",
        python_callable=etl_process,
    )

    # Set the task sequence
    run_etl