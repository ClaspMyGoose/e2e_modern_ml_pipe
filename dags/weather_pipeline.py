from datetime import datetime, timedelta 
import os 
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.utils.log.logging_mixin import LoggingMixin

extract_mount_path_json = os.getenv('EXTRACT_MOUNT_PATH_JSON')
extract_mount_path_csv = os.getenv('EXTRACT_MOUNT_PATH_CSV')
process_mount_path = os.getenv('PROCESS_MOUNT_PATH')
duckdb_mount_path = os.getenv('DUCKDB_MOUNT_PATH')
dbt_project_mount_path = os.getenv('DBT_PROJECT_MOUNT_PATH')
process_output_mount_path = os.getenv('PROCESS_OUTPUT_MOUNT_PATH')

default_args = {
    'owner': 'you',
    'depends_on_past': False, 
    'start_date': datetime(2025,7,25), 
    'email_on_failure': False,
    'email_on_retry': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='Daily weather data extraction',
    schedule_interval='@daily',
    catchup=False
)


extract_weather = BashOperator(
    task_id='extract_weather_data',
    bash_command='python /opt/airflow/scripts/extract_weather_data.py',
    dag=dag 
)

process_weather = DockerOperator(
    task_id='process_weather_data',
    image='spark_python_image',
    mount_tmp_dir=False,
    mounts=[
        Mount(source=extract_mount_path_json, target='/app/data/json/', type='bind'),
        Mount(source=extract_mount_path_csv, target='/app/data/csv/', type='bind'),
        Mount(source=process_mount_path, target='/app/processed_data/', type='bind')
    ],
    dag=dag
)

load_weather = DockerOperator(
    task_id='load_staging_daily_monthly',
    image='duckdb_dbt_image',
    environment={
        'DUCKDB_PATH': '/app/db/weather_data.duckdb'
    },
    mount_tmp_dir=False,
    working_dir='/app/dbt_weather',
    mounts=[
        Mount(source=duckdb_mount_path, target='/app/db/weather_data.duckdb', type='bind'),
        Mount(source=dbt_project_mount_path, target='/app/dbt_weather/', type='bind'),
        Mount(source=process_output_mount_path, target='/app/processed_data/weather_output', type='bind')
    ],
    command='bash -c "dbt clean && dbt run"',
    dag=dag  
)

extract_weather >> process_weather >> load_weather 



# remember to add my env variable in the DockerOperator call to dbt container 

