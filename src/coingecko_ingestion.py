import requests
import json
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook # IMPORT FOR GCS UPLOAD

def fetch_and_persist_json():
    # 1. Data Ingestion
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # 2. Local Persistence (storing a copy on the local filesystem)
    base_path = "/opt/airflow/data/bronze"
    os.makedirs(base_path, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"crypto_data_{timestamp}.json"
    local_path = f"{base_path}/{filename}"
    
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    # --- UPLOAD TO GOOGLE CLOUD STORAGE ---
    try:
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') 
        bucket_name = 'crypto-data-pipeline-2026-storage'
        
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=f"raw_data/{filename}", # Uploads to raw_data folder in the cloud
            filename=local_path # Sources from the local path where we just saved it
        )
        print(f"--- CLOUD UPLOAD SUCCESSFUL: gs://{bucket_name}/raw_data/{filename} ---")
    except Exception as e:
        print(f"!!! GCS UPLOAD ERROR: {e}")
        raise # Fail the task if cloud upload fails

# DAG configuration
with DAG(
    dag_id='crypto_ingestion_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['crypto', 'bronze']
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_and_persist_to_bronze',
        python_callable=fetch_and_persist_json
    )