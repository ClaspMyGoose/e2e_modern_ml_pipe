from datetime import datetime, timedelta 

from airflow import DAG
from airflow.operators.bash import BashOperator


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

process_weather = BashOperator(
    task_id='process_weather_data',
    bash_command='python /opt/airflow/scripts/spark_weather_processing.py',
    dag=dag
)

extract_weather >> process_weather

