from src.config.logging import logger
from google.cloud import storage
from src.config.setup import *


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