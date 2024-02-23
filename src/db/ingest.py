from src.config.logging import logger
from google.cloud import storage
from src.config.setup import *
from datetime import datetime, timezone


def find_most_recent_folder(bucket_name: str):
    """Find the most recent 'folder' in a GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    prefixes = set()  # To store unique 'folder' prefixes
    most_recent_prefix = None
    most_recent_time = datetime.min.replace(tzinfo=timezone.utc)  # Make it timezone-aware

    try:
        blobs = bucket.list_blobs(delimiter='/')
        for page in blobs.pages:
            for prefix in page.prefixes:
                prefixes.add(prefix)
        
        for prefix in prefixes:
            # Fetch the blob that corresponds to the 'folder' to get its creation time
            blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/'))
            if blobs:
                creation_time = max(blob.time_created for blob in blobs)
                if creation_time > most_recent_time:
                    most_recent_time = creation_time
                    most_recent_prefix = prefix

        if most_recent_prefix:
            logger.info(f"Most recent folder: {most_recent_prefix} (Created: {most_recent_time})")
            return most_recent_prefix
        else:
            logger.info("No folders found in the bucket.")
            return None
    except Exception as e:
        logger.error(f"Failed to find the most recent folder: {e}")
        return None


def list_blobs_with_prefix(bucket_name: str, prefix: str, delimiter=None):
    """Lists all the blobs in the bucket with a given prefix."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    try:
        for blob in blobs:
            print(f"Blob: {blob.name}")
            # Optionally, download or process the blob here
    except Exception as e:
        logger.error(f"Failed to list or process blobs: {e}")


if __name__ == '__main__':
    
    bucket_name = 'vais-app-builder'
    most_recent_folder = find_most_recent_folder(bucket_name)
    if most_recent_folder:
        print(f"The most recent folder is: {most_recent_folder}")
    else:
        print("No recent folder found or error occurred.")
    try:
        list_blobs_with_prefix(bucket_name, most_recent_folder)
    except Exception as e:
        logger.error(f"Failed to list blobs in GCS bucket: {e}")
