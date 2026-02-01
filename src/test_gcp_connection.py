from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook

def test_list_clusters():
    hook = DataprocHook(gcp_conn_id='google_cloud_default')
    # Configuration: Directly requesting project data from the hook
    project_id = 'crypto-data-pipeline-2026'
    region = 'europe-west1'
    
    client = hook.get_cluster_client(region=region)
    
    # Legacy versions require parameters to be passed this way:
    clusters = client.list_clusters(
        project_id=project_id,
        region=region
    )
    
    print(f"--- Connection Successful! ---")
    for cluster in clusters:
        print(f"Found cluster: {cluster.cluster_name}")

with DAG(
    dag_id='test_gcp_connection_v1',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='test_dataproc_connection',
        python_callable=test_list_clusters
    )