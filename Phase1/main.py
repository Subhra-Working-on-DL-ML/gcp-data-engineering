import tarfile
import json
import os
from google.cloud import storage, pubsub_v1

# Configuration
TOPIC_NAME = "flight_transactions"
PROJECT_ID = "flight-de-project"
BUCKET_NAME = "flight_data_store"
FILE_NAME = "input_data.tar.gz"  # File in GCS
LOCAL_TAR_FILE = "input_data.tar.gz"  # Local file name
EXTRACTED_DIR = "extracted_data"

def download_file_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a file from GCS to the local filesystem.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} to {destination_file_name}")

def extract_tar_gz(file_path, extract_dir):
    """
    Extracts a tar.gz file into a specified directory.
    """
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    with tarfile.open(file_path, "r:gz") as tar:
        tar.extractall(path=extract_dir)
        print(f"Extracted files to {extract_dir}")

def publish_to_pubsub(file_dir):
    """
    Reads JSON files from the directory and publishes each record to Pub/Sub.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

    for file_name in os.listdir(file_dir):
        if file_name.endswith(".json") and not file_name.startswith('._'):
            file_path = os.path.join(file_dir, file_name)
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    try:
                        message = json.dumps(json.loads(line)).encode('utf-8')
                        publisher.publish(topic_path, message)
                        # print(f"Published message: {message}")
                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON line: {line}")
    print("Published messages")
if __name__ == "__main__":
    # Step 1: Download the tar.gz file from GCS
    download_file_from_gcs(BUCKET_NAME, FILE_NAME, LOCAL_TAR_FILE)

    # Step 2: Extract the tar.gz file
    extract_tar_gz(LOCAL_TAR_FILE, EXTRACTED_DIR)

    # Step 3: Publish the extracted data to Pub/Sub
    publish_to_pubsub(EXTRACTED_DIR)
