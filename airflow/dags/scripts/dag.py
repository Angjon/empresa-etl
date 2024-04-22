from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from scripts.download_data import download_data
from scripts.extract_data import extract_data
from scripts.transform_bronze import transform_bronze
from scripts.transform_silver import transform_silver

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_process',
    default_args=default_args,
    description='Orchestrate ETL process',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['etl']
)

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_bronze_task = PythonOperator(
    task_id='transform_bronze',
    python_callable=transform_bronze,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    dag=dag,
)

# Set dependencies
download_task >> extract_task >> transform_bronze_task >> transform_silver_task
