from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 24), # Set to one day earlier to ensure scheduled runs are active
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_data_vault_load',
    default_args=default_args,
    description='Spark job that populates Data Vault tables daily at 13:00',
    schedule_interval='0 13 * * *',  # Minute: 0, Hour: 13 -> runs every day at 1:00 PM
    catchup=True,                    # Enables backfilling missed runs after system downtime
    tags=['silver', 'spark', 'datavault'],
) as dag:

    # This Task triggers the vault_loader.py script
    load_silver_vault = SparkSubmitOperator(
        task_id='load_hub_and_satellite',
        application='/opt/airflow/dags/vault_loader.py',
        conn_id='spark_default',
        verbose=True
    )