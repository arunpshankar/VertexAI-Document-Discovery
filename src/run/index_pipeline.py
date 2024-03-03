from src.db.create import create_engine_with_connection_pool
from src.batch.create import process_dataframe_chunks
from src.batch.ingest import find_most_recent_folder
from src.batch.ingest import list_blobs_with_prefix
from src.batch.ingest import parse_blob_contents
from src.search.index import create_request_body
from src.search.index import post_target_sites
from src.search.index import create_search_app
from src.search.index import create_data_store
from src.batch.create import load_dataframe
from src.db.create import insert_entity_url
from sqlalchemy.exc import SQLAlchemyError
from src.search.index import chunk_data
from src.utils.gcp import upload_to_gcs
from src.db.create import create_table
from src.config.logging import logger 
from sqlalchemy.engine import Engine
from src.config.setup import config
from datetime import datetime 
from typing import Optional
from typing import Tuple
from typing import List 
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
    Finds the most recent folder in a GCS bucket, processes all blobs within it by parsing their contents, 
    and performs subsequent data processing tasks like database insertions and search index updates.

    Parameters:
    - bucket_name (str): The name of the GCS bucket.

    Returns:
    None
    """
    try:
        engine = create_engine_with_connection_pool()
        create_table(engine)
        most_recent_folder = find_most_recent_folder(bucket_name)
        if not most_recent_folder:
            logger.info("No recent folder found.")
            return

        logger.info(f"Processing folder: {most_recent_folder}")
        process_blobs(bucket_name, most_recent_folder, engine)
    except Exception as e:
        logger.error(f"Error processing the most recent data: {e}", exc_info=True)


def process_blobs(bucket_name: str, folder: str, engine: Engine) -> None:
    """
    Iterates over and processes each blob within a specified folder of the bucket.

    Parameters:
    - bucket_name (str): The GCS bucket name.
    - folder (str): The folder name in the bucket.
    - engine (Engine): Database engine instance for operations.

    Returns:
    None
    """
    blobs = list_blobs_with_prefix(bucket_name, folder)
    for blob in blobs:
        process_blob(blob, bucket_name, engine)
        # break  # Uncomment this line for testing


def process_blob(blob, bucket_name: str, engine: Engine) -> None:
    """
    Parses a single blob's contents for processing, including database insertion and further data processing tasks.

    Parameters:
    - blob: Blob object to be processed.
    - bucket_name (str): The GCS bucket name.
    - engine (Engine): Database engine instance.

    Returns:
    None
    """
    try:
        site_urls, batch_id = parse_and_store_blob_contents(blob, bucket_name, engine)
        if batch_id:
            initiate_data_indexing_and_search(batch_id, site_urls)
    except Exception as e:
        logger.error(f"Error processing blob {blob.name}: {e}", exc_info=True)


def parse_and_store_blob_contents(blob, bucket_name: str, engine: Engine) -> Tuple[List[str], Optional[str]]:
    """
    Parses blob's contents and stores relevant data in the database.

    Parameters:
    - blob: Blob object to parse.
    - bucket_name (str): GCS bucket name.
    - engine (Engine): Database engine instance.

    Returns:
    Tuple[List[str], Optional[str]]: A list of URLs and a batch ID.
    """
    site_urls = []
    batch_id = None
    contents = parse_blob_contents(blob, bucket_name) 
    for content in contents:
        entity = content.get('entity')
        url = content.get('url')
        country = content.get('country')
        batch_id = content.get('batch_id', batch_id)  # Use existing batch_id if present
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cloud_storage_uri = content.get('cloud_storage_uri')

        entry = {
            "entity": entity,
            "url": url,
            "country": country,
            "batch_id": batch_id,
            "created_at": created_at,
            "cloud_storage_uri": cloud_storage_uri
        }
        try:
            insert_entity_url(engine, entry)
            site_urls.append(url)
        except SQLAlchemyError as e:
            logger.error(f"Database insertion failed for {url}: {e}", exc_info=True)
    return site_urls, batch_id


def initiate_data_indexing_and_search(batch_id: str, site_urls: List[str]) -> None:
    """
    Initiates indexing and search-related processing for a batch of site URLs.

    Parameters:
    - batch_id (str): The batch ID.
    - site_urls (List[str]): List of site URLs.

    Returns:
    None
    """
    try:
        data_store_response = create_data_store(batch_id)
        logger.info(f"Data store created with response: {data_store_response}")

        data = create_request_body(site_urls, batch_id)
        chunks = chunk_data(data['requests'], 20)
        for chunk in chunks:
            response = post_target_sites({'requests': chunk}, batch_id)
            if response:
                logger.info(f"Successfully posted target sites for batch {batch_id}")
            else:
                logger.error(f"Failed to post target sites for batch {batch_id}")

        search_app_response = create_search_app(batch_id)
        logger.info(f"Search app created with response: {search_app_response}")
    except Exception as e:
        logger.error(f"Error in data indexing and search initiation for batch {batch_id}: {e}", exc_info=True)


def chunk_data(data, chunk_size: int) -> List[List[dict]]:
    """Splits data into chunks of specified size."""
    return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]


def main():
    """
    Main function to orchestrate loading, processing, uploading, and parsing data.

    Returns:
    None
    """
    load_and_process_input_data(config.INPUT_FILE_PATH, config.LOCAL_OUTPUT_PATH)
    upload_chunks_to_gcs(config.LOCAL_OUTPUT_PATH, config.BUCKET)
    process_most_recent_data(config.BUCKET)


if __name__ == '__main__':
    main()
