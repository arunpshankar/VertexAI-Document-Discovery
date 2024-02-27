from src.config.logging import logger
from google.cloud import storage
from src.config.setup import *
import datetime
import os 


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    try:
        blob.upload_from_filename(source_file_path)
        logger.info(f"File {source_file_path} uploaded to {destination_blob_name}.")
    except Exception as e:
        logger.error(f"Failed to upload file to GCS: {e}")



if __name__ == '__main__':
    bucket_name = 'vais-app-builder'
    timestamp_folder = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    output_dir = './data/batch/'
    try:
        for filename in os.listdir(output_dir):
            source_file_path = os.path.join(output_dir, filename)
            destination_blob_name = f"{timestamp_folder}/{filename}"
            upload_to_gcs(bucket_name, source_file_path, destination_blob_name)
    except Exception as e:
        logger.error(f"Failed to upload files to GCS: {e}")
