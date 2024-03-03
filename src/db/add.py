
from batch.ingest import find_most_recent_folder
from batch.ingest import list_blobs_with_prefix
from google.cloud.sql.connector import Connector
from batch.ingest import parse_blob_contents
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine
from src.config.logging import logger
from src.config.setup import config
from datetime import datetime
from typing import Optional
from typing import Dict
from typing import List
from typing import Any
import sqlalchemy
import subprocess
import requests


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


def create_urls_table(engine: Engine):
    """
    Creates the 'entity_urls' table in the database if it doesn't exist.

    Args:
        engine: A SQLAlchemy engine object.

    Raises:
        SQLAlchemyError: If there's an issue executing the table creation command.
    """
    create_table_statement = sqlalchemy.text("""
        CREATE TABLE IF NOT EXISTS entity_urls (
            entity VARCHAR(255) NOT NULL,
            url VARCHAR(255) NOT NULL,
            country VARCHAR(255) NOT NULL,
            batch_id VARCHAR(255) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            cloud_storage_uri VARCHAR(255),
            PRIMARY KEY (entity, country), -- Composite primary key for uniqueness
            INDEX idx_entity (entity)
            -- Efficient queries on entity, composite primary key handles entity-country combinations
        );
    """)

    try:
        with engine.connect() as connection:
            connection.execute(create_table_statement)
            connection.commit()
            logger.info("Table 'entity_urls' created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to create table 'entity_urls': {e}")
        raise


def insert_entity_url(engine: Engine, entity_url_data: dict):
    """
    Inserts a new entry into the 'entity_urls' table.

    Args:
        engine: A SQLAlchemy engine object.
        entity_url_data: A dictionary containing the column data for the new entry.
    """
    insert_stmt = sqlalchemy.text(
        "INSERT INTO entity_urls (entity, url, country, batch_id, "
        "created_at, cloud_storage_uri) "
        "VALUES (:entity, :url, :country, :batch_id, "
        ":created_at, :cloud_storage_uri)"
    )
    try:
        with engine.connect() as connection:
            # Pass parameters as a dictionary directly
            connection.execute(insert_stmt, entity_url_data)
            connection.commit()
            logger.info("New entity_url entry inserted successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert entity_url entry: {e}")
        raise



engine = create_engine_with_connection_pool()
create_urls_table(engine)
# Inserting data into entity_urls table

bucket_name = 'vais-app-builder'
most_recent_folder = find_most_recent_folder(bucket_name)


for blob in list_blobs_with_prefix(bucket_name, most_recent_folder):
    # Parse each blob's contents
    site_urls = []
    batch_id = None

    for info in parse_blob_contents(blob, bucket_name):
        batch_id = info['batch_id']
        entity = info['name']
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
            "entity": entity,
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


        #insert_entity_url(engine, entry)
    
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
