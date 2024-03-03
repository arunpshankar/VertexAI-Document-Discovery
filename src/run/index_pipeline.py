from src.db.create import create_engine_with_connection_pool
from src.batch.create import process_dataframe_chunks
from src.batch.ingest import find_most_recent_folder
from src.batch.ingest import list_blobs_with_prefix
from src.batch.ingest import parse_blob_contents
from src.batch.create import load_dataframe
from src.db.create import insert_entity_url
from src.utils.gcp import upload_to_gcs
from src.db.create import create_table
from src.config.logging import logger 
from src.config.setup import config
from datetime import datetime 
import os 


def load_and_process_input_data(input_file_path: str, local_output_path: str) -> None:
    """
    Load input data from a CSV file, process it into chunks, and write those chunks to a local directory.

    Parameters:
    - input_file_path: The file path of the input CSV.
    - local_output_path: The directory path where chunked dataframes will be stored.

    Returns:
    None
    """
    try:
        df = load_dataframe(input_file_path)
        process_dataframe_chunks(df, local_output_path)
        logger.info("Dataframe loaded and processed successfully.")
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")


def upload_chunks_to_gcs(local_output_path: str, bucket_name: str) -> None:
    """
    Uploads processed data chunks from a local directory to Google Cloud Storage (GCS).

    Parameters:
    - local_output_path: The directory path where chunked dataframes are stored.
    - bucket_name: The name of the GCS bucket where files will be uploaded.

    Returns:
    None
    """
    timestamp_folder = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    try:
        for filename in os.listdir(local_output_path):
            source_file_path = os.path.join(local_output_path, filename)
            destination_blob_name = f"{timestamp_folder}/{filename}"
            upload_to_gcs(bucket_name, source_file_path, destination_blob_name)
        logger.info("Files uploaded to GCS successfully.")
    except Exception as e:
        logger.error(f"Failed to upload files to GCS: {e}")


def process_most_recent_data(bucket_name: str) -> None:
    """
    Finds the most recent folder in GCS, lists blobs with the specified prefix, and parses each blob's contents.

    Parameters:
    - bucket_name: The name of the GCS bucket to search.

    Returns:
    None
    """
    engine = create_engine_with_connection_pool()
    create_table(engine)
    most_recent_folder = find_most_recent_folder(bucket_name)
    if most_recent_folder:
        logger.info(f"The most recent folder is: {most_recent_folder}")
        for blob in list_blobs_with_prefix(bucket_name, most_recent_folder):
            site_urls = []
            batch_id = None
            try:
                for info in parse_blob_contents(blob, bucket_name):
                    # Parse each blob's contents
                    batch_id = info['batch_id']
                    entity = info['name']
                    url = info['root_url']
                    country = info['country']
                    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    cloud_storage_uri = info['cloud_storage_uri']
                    site_urls.append(url)

                    print(entity)

                    entry = {
                        "entity": entity,
                        "url": url,
                        "country": country,
                        "batch_id": batch_id,
                        "created_at": created_at,
                        "cloud_storage_uri": cloud_storage_uri
                    }
                    insert_entity_url(engine, entry)
    
            except Exception as e:
                logger.error(f"Failed to parse blob {blob.name}: {e}")
            break # If you want to process all blobs, remove this break
    else:
        logger.info("No recent folder found or error occurred.")
        

def main():
    """
    Main function to orchestrate loading, processing, uploading, and parsing data.

    Returns:
    None
    """
    #load_and_process_input_data(config.INPUT_FILE_PATH, config.LOCAL_OUTPUT_PATH)
    #upload_chunks_to_gcs(config.LOCAL_OUTPUT_PATH, config.BUCKET)
    process_most_recent_data(config.BUCKET)

if __name__ == '__main__':
    main()