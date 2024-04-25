from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from cnpj.scripts.download_data import download_data
from cnpj.scripts.extract_data import extract_data
from cnpj.scripts.transform_bronze import transform_bronze
from cnpj.scripts.transform_silver import transform_silver
from airflow.models import Variable

RAW_PATH = Variable.get("raw_path_var")
BRONZE_PATH = Variable.get("bronze_path_var")
DOMAIN_PATH = Variable.get("domain_path_var")
GOLD_PATH = Variable.get('gold_path_var')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Run it daily at midnight
dag = DAG(
    'etl_process',
    default_args=default_args,
    description='Orchestrate ETL process',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    tags=['etl']
)


download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
    op_kwargs={"save_path":RAW_PATH}
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
    op_kwargs={"save_path":BRONZE_PATH}
)

transform_bronze_task = PythonOperator(
    task_id='transform_bronze',
    python_callable=transform_bronze,
    dag=dag,
    op_kwargs={"save_path":DOMAIN_PATH}
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    dag=dag,
    op_kwargs={"save_path":GOLD_PATH}
)

# Set dependencies
download_task >> extract_task >> transform_bronze_task >> transform_silver_task
