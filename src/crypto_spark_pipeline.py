from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
# IMPORT FOR BIGQUERY LOADING
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# --- CONFIGURATION ---
PROJECT_ID = "crypto-data-pipeline-2026"
REGION = "europe-west1"
CLUSTER_NAME = "crypto-spark-cluster-temp"
PYSPARK_URI = "gs://crypto-data-pipeline-2026-storage/scripts/transform_crypto_data.py"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# --- DAG DEFINITION ---
with DAG(
    dag_id="crypto_dataproc_orchestration_v1",
    default_args=default_args,
    # Scheduled to run every 30 minutes for automated data collection
    schedule_interval='*/30 * * * *', 
    start_date=days_ago(1),
    catchup=False,
    tags=['crypto'],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {
                "num_instances": 1, 
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}
            },
            "worker_config": {
                "num_instances": 2, 
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32}
            },
        },
    )

    submit_job = DataprocSubmitPySparkJobOperator(
        task_id="run_spark_job",
        main=PYSPARK_URI,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

    # --- TASK 5.3: LOAD DATA INTO BIGQUERY ---
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='crypto-data-pipeline-2026-storage',
        source_objects=['processed_data/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.crypto_data.prices',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id='google_cloud_default',
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done"
    )

    # UPDATED EXECUTION ORDER:
    create_cluster >> submit_job >> load_to_bq >> delete_cluster