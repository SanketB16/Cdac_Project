import os
import sys
import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add ETL pipeline path dynamically (works in Docker & local)
sys.path.append(os.path.join(os.path.dirname(__file__), '../etl_scripts'))

# Import custom ETL functions
from excel_to_deltalake import convert_all_excels_to_deltalake
from spark_clean_and_load import spark_clean_and_load

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL Pipeline: Excel → Delta Lake → Spark → Cleaned MySQL',
    tags=['ETL', 'Spark', 'MySQL']
) as dag:

    task_excel_to_deltalake = PythonOperator(
        task_id='convert_excel_to_deltalake',
        python_callable=convert_all_excels_to_deltalake
    )

    task_spark_clean_and_load = PythonOperator(
        task_id='spark_clean_and_load',
        python_callable=spark_clean_and_load
    )

    task_excel_to_deltalake >> task_spark_clean_and_load
