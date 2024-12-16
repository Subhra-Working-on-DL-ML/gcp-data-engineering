from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import tarfile
import os

# Constants
PROJECT_ID = "flight-de-project"
BUCKET_NAME = "flight_data_store"
FILE_NAME = "input_data.tar.gz"
EXTRACTED_DIR = "/tmp/extracted_data"
PUBSUB_TOPIC = "flight_transactions"
DATAFLOW_SCRIPT = "gs://flight_data_store/dataflow_pipeline_4.py"
TEMPLATE_PATH = f"gs://{BUCKET_NAME}/templates/dataflow_template"

def extract_tar_file():
    """
    Custom Python function to extract a tar.gz file.
    """
    tar_path = f"/tmp/{FILE_NAME}"
    if not os.path.exists(EXTRACTED_DIR):
        os.makedirs(EXTRACTED_DIR)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(path=EXTRACTED_DIR)
    print(f"Extracted files to {EXTRACTED_DIR}")

with DAG(
    "flight_data_pipeline",
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
        "retries": 1,
    },
    schedule_interval=None,  # Triggered manually or via an external process
    catchup=False,
) as dag:

    # Task 1: Download file from GCS
    download_file = GCSToLocalFilesystemOperator(
    task_id="download_file_from_gcs",
    bucket=BUCKET_NAME,
    object_name=FILE_NAME,
    filename=f"/tmp/{FILE_NAME}",
    )


    # Task 2: Extract tar.gz file
    extract_file = PythonOperator(
        task_id="extract_tar_file",
        python_callable=extract_tar_file,
    )

    # Task 3: Publish data to Pub/Sub
    publish_data = PubSubPublishMessageOperator(
        task_id="publish_data_to_pubsub",
        project_id=PROJECT_ID,
        topic=PUBSUB_TOPIC,
        messages=[{"data": b"Data processing initiated."}],
    )

    # Task 4: Trigger the Dataflow job
    


# Task definition
    start_dataflow = DataflowTemplatedJobStartOperator(
        task_id="trigger_dataflow",
        job_name="flight-dataflow-job",  # Unique name for the Dataflow job
        template=TEMPLATE_PATH,
        location="europe-west4",
        parameters={
            "runner": "DataflowRunner",
            "project": PROJECT_ID,
            "region": "europe-west4",
            "temp_location": f"gs://{BUCKET_NAME}/temp",
            "staging_location": f"gs://{BUCKET_NAME}/staging",
        },
    )


    # DAG Dependencies
    download_file >> extract_file >> publish_data >> start_dataflow
