
from src.db.ingest import find_most_recent_folder
from src.db.ingest import list_blobs_with_prefix
from google.cloud.sql.connector import Connector
from src.db.ingest import parse_blob_contents

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine
from src.config.logging import logger
from src.config.setup import config
from datetime import datetime
import sqlalchemy

import subprocess
import requests

from typing import Optional, Dict, Any, List


# Initialize connection parameters
instance_connection_name = f"{config.PROJECT_ID}:{config.REGION}:{config.CLOUD_SQL_INSTANCE}"
logger.info(f"Connection name: {instance_connection_name}")

# Initialize Connector object
connector = Connector()


def get_connection() -> sqlalchemy.engine.base.Connection:
    """
    Establishes a connection to the Cloud SQL instance.

    Returns:
        A connection object to the Cloud SQL database.
    """
    try:
        connection = connector.connect(
            instance_connection_name,
            "pymysql",
            user=config.CLOUD_SQL_USERNAME,
            password=config.CLOUD_SQL_PASSWORD,
            db=config.CLOUD_SQL_DATABASE
        )
        logger.info("Successfully established connection to Cloud SQL.")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to Cloud SQL: {e}")
        raise


def create_engine_with_connection_pool() -> Engine:
    """
    Creates a SQLAlchemy engine with a connection pool using the `get_connection` function.

    Returns:
        A SQLAlchemy engine object.
    """
    engine = sqlalchemy.create_engine("mysql+pymysql://", creator=get_connection)
    logger.info("SQLAlchemy engine with connection pool created successfully.")
    return engine


def create_university_urls_table(engine: Engine):
    """
    Creates the 'university_urls' table in the database if it doesn't exist.

    Args:
        engine: A SQLAlchemy engine object.

    Raises:
        SQLAlchemyError: If there's an issue executing the table creation command.
    """
    create_table_statement = sqlalchemy.text("""
        CREATE TABLE IF NOT EXISTS university_urls (
            university VARCHAR(255) NOT NULL,
            url VARCHAR(255) NOT NULL,
            country VARCHAR(255) NOT NULL,
            batch_id VARCHAR(255) NOT NULL,
            data_store_name VARCHAR(255) NOT NULL,
            data_store_id VARCHAR(255) NOT NULL,
            app_name VARCHAR(255) NOT NULL,
            app_id VARCHAR(255) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            index_status VARCHAR(50) NOT NULL,
            cloud_storage_uri VARCHAR(255),
            PRIMARY KEY (university, country), -- Composite primary key for uniqueness
            INDEX idx_university (university)
            -- Efficient queries on university, composite primary key handles university-country combinations
        );
    """)

    try:
        with engine.connect() as connection:
            connection.execute(create_table_statement)
            connection.commit()
            logger.info("Table 'university_urls' created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to create table 'university_urls': {e}")
        raise


def insert_university_url(engine: Engine, university_url_data: dict):
    """
    Inserts a new entry into the 'university_urls' table.

    Args:
        engine: A SQLAlchemy engine object.
        university_url_data: A dictionary containing the column data for the new entry.
    """
    insert_stmt = sqlalchemy.text(
        "INSERT INTO university_urls (university, url, country, batch_id, data_store_name, "
        "data_store_id, app_name, app_id, created_at, index_status, cloud_storage_uri) "
        "VALUES (:university, :url, :country, :batch_id, :data_store_name, :data_store_id, "
        ":app_name, :app_id, :created_at, :index_status, :cloud_storage_uri)"
    )
    try:
        with engine.connect() as connection:
            # Pass parameters as a dictionary directly
            connection.execute(insert_stmt, university_url_data)
            connection.commit()
            logger.info("New university_url entry inserted successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert university_url entry: {e}")
        raise


def create_search_app(data_store_id) -> Optional[Dict[str, Any]]:
    """
    Creates a site search application using the Google Discovery Engine API.

    This function constructs a POST request to the Google Discovery Engine API to create a
    new site search app for a list of 50 company urls webpages 

    Returns:
        dict: A dictionary containing the response data from the API if the request is successful.
        None: If the request fails.
    """
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/engines?engineId={data_store_id}"

    # Headers for the request
    headers = {
        "Authorization": f"Bearer {config.ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": config.PROJECT_ID
    }

    # Request payload
    data = {
        "displayName": f'moodys_site_search_{data_store_id}',
        "dataStoreIds": [data_store_id],
        "solutionType": "SOLUTION_TYPE_SEARCH", 
        "searchEngineConfig": {
            "searchTier": "SEARCH_TIER_ENTERPRISE",
            
        }
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        logger.info(f"Site search app created successfully for batch {data_store_id}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create document search app: {str(e)}")
        return None
    

def fetch_access_token() -> Optional[str]:
    """
    Fetch an access token for authentication.
    Returns:
        Optional[str]: The fetched access token if successful, None otherwise.
    """
    cmd = ["gcloud", "auth", "print-access-token"]
    try:
        token = subprocess.check_output(cmd).decode('utf-8').strip()
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch access token: {e}")
        return None

def create_headers() -> Dict[str, str]:
    """
    Create headers for the HTTP request including authorization.
    Returns:
        Dict[str, str]: Headers for the request.
    """
    token = fetch_access_token()
    if token is None:
        raise RuntimeError("Failed to obtain access token")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": config.PROJECT_ID
    }


def create_request_body(uri_patterns: List[str], data_store_id: str) -> Dict[str, Any]:
    """
    Create the request body for target sites batch creation.
    Args:
        uri_patterns (List[str]): A list of URI patterns to include in the target sites.
    Returns:
        Dict[str, Any]: The constructed request body.
    """
    requests_list = [{
        'parent': f'projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores/{data_store_id}/siteSearchEngine',
        'targetSite': {'providedUriPattern': uri_pattern, 'type': 'INCLUDE', 'exactMatch': True}
    } for uri_pattern in uri_patterns]
    
    return {"requests": requests_list}


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

def create_data_store(batch_id):
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores?dataStoreId={batch_id}"
    headers = {
            "Authorization": f"Bearer {fetch_access_token()}",
            "Content-Type": "application/json",
            "X-Goog-User-Project": config.PROJECT_ID
        }
    data = {
        'displayName': f'moodys_site_search_{batch_id}',
        'industryVertical': 'GENERIC',
        'solutionTypes': ['SOLUTION_TYPE_SEARCH'],
        'contentConfig': 'PUBLIC_WEBSITE',
        'searchTier': 'STANDARD'
        
    }
    response = requests.post(url, headers=headers, json=data)
    return response


def post_target_sites(data: Dict[str, Any], data_store_id: str) -> Optional[Dict[str, Any]]:
    """
    Post target sites to the specified URL using the Google Cloud Discovery Engine API.
    Args:
        data (Dict[str, Any]): The data to be posted as JSON.
    Returns:
        Optional[Dict[str, Any]]: The JSON response if successful, None otherwise.
    """
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores/{data_store_id}/siteSearchEngine/targetSites:batchCreate"
    headers = create_headers()
    
    
    response = requests.post(url, headers=headers, json=data)
    #response.raise_for_status()  # This will raise an HTTPError if the response was an error
    return response.json()


def chunk_data(data, size):
    """Yield successive size chunks from data."""
    for i in range(0, len(data), size):
        yield data[i:i + size]


engine = create_engine_with_connection_pool()
create_university_urls_table(engine)
# Inserting data into university_urls table

bucket_name = 'vais-app-builder'
most_recent_folder = find_most_recent_folder(bucket_name)


for blob in list_blobs_with_prefix(bucket_name, most_recent_folder):
    # Parse each blob's contents
    site_urls = []
    batch_id = None

    for info in parse_blob_contents(blob, bucket_name):
        batch_id = info['batch_id']
        university = info['name']
        url = info['root_url']
        country = info['country']
        data_store_name = batch_id
        data_store_id = batch_id
        app_name = batch_id
        app_id = batch_id
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        index_status = 'Indexed'
        cloud_storage_uri = info['cloud_storage_uri']
        site_urls.append(url)

        entry = {
            "university": university,
            "url": url,
            "country": country,
            "batch_id": batch_id,
            "data_store_name": data_store_name,
            "data_store_id": data_store_id,
            "app_name": app_name,
            "app_id": app_id,
            "created_at": created_at,
            "index_status": "Indexed",
            "cloud_storage_uri": cloud_storage_uri
        }


        #insert_university_url(engine, entry)
    
    response = create_data_store(batch_id)
    #print(response)

    data = create_request_body(site_urls, batch_id)
    
    # Example usage
    chunks = list(chunk_data(data['requests'], 20))
    for chunk in chunks:
        #print(chunk)
        print('_' * 100)
    
        batch_data = {'requests': chunk}
        response = post_target_sites(batch_data, batch_id)
        if response is not None:
            logger.info(f"Successfully posted target sites: {response}")
        else:
            logger.error("Failed to post target sites.")
    response = create_search_app(batch_id)
    logger.info(response)
    break
