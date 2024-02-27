from src.config.logging import logger
from google.cloud import storage
from src.config.setup import *
from datetime import datetime
from datetime import timezone
from typing import Generator
from typing import Dict
from typing import Any 
import json 


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
            return most_recent_prefix
        else:
            logger.info("No folders found in the bucket.")
            return None
    except Exception as e:
        logger.error(f"Failed to find the most recent folder: {e}")
        return None


def extract_batch_id(filepath: str) -> str:
    """
    Extracts the batch ID from a filepath.
    
    Args:
    - filepath: The filepath in the format 'path/to/batch_XXXX_YYYY.jsonl'.
    
    Returns:
    - The batch ID in the format 'XXXX_YYYY'.
    """
    # Split the filepath by '/' and pick the last element
    filename = filepath.split('/')[-1]
    # Split the filename by '_', pick the parts with numbers, and join them back
    parts = filename.split('_')
    batch_id = '_'.join(parts[1:3]).replace('.jsonl', '')
    return batch_id


def list_blobs_with_prefix(bucket_name: str, prefix: str, delimiter=None) -> Generator[storage.Blob, None, None]:
    """Yield Google Cloud Storage Blob objects in the bucket with a given prefix."""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    for blob in blobs:
        # Make sure we're only yielding .jsonl files
        if blob.name.endswith('.jsonl'):
            yield blob
            

def parse_blob_contents(blob: storage.Blob, bucket_name: str) -> Generator[Dict[str, Any], None, None]:
    """Yield dictionaries from a JSONL file represented by a Blob object."""
    # Download the blob's contents as text
    blob_as_text = blob.download_as_text()
    for line in blob_as_text.splitlines():
        try:
            # Parse each line as JSON and yield the resulting dictionary
            batch_id = extract_batch_id(blob.name)

            info = json.loads(line)
            info['batch_id'] = batch_id
            info['cloud_storage_uri'] = f'gs://{bucket_name}/{blob.name}'
            
            yield info
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON from blob {blob.name}: {e}")


if __name__ == '__main__':
    bucket_name = 'vais-app-builder'
    most_recent_folder = find_most_recent_folder(bucket_name)
    if most_recent_folder:
        print(f"The most recent folder is: {most_recent_folder}")
    else:
        print("No recent folder found or error occurred.")
    

    # List blobs with the specified prefix
    for blob in list_blobs_with_prefix(bucket_name, most_recent_folder):
        # Parse each blob's contents
        try:
            for info in parse_blob_contents(blob, bucket_name):
                print(info)
        except Exception as e:
            logger.error(f"Failed to parse blob {blob.name}: {e}")
        break