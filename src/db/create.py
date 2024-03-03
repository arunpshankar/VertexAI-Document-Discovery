
from google.cloud.sql.connector import Connector
from sqlalchemy.engine.base import Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine 
from src.config.logging import logger
from sqlalchemy import create_engine
from src.config.setup import config
from sqlalchemy import text


# Global variables
INSTANCE_CONNECTION_NAME = f"{config.PROJECT_ID}:{config.REGION}:{config.CLOUD_SQL_INSTANCE}"

# Initialize Connector object globally to reuse
connector = Connector()


def get_connection() -> Connection:
    """
    Establishes a connection to the Cloud SQL instance.

    Returns:
        A connection object to the Cloud SQL database.
    """
    try:
        connection = connector.connect(
            INSTANCE_CONNECTION_NAME,
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
    engine = create_engine("mysql+pymysql://", creator=get_connection)
    logger.info("SQLAlchemy engine with connection pool created successfully.")
    return engine


def create_table(engine: Engine):
    """
    Creates the 'entity_urls' table in the database if it doesn't exist.

    Args:
        engine: A SQLAlchemy engine object.
    """
    create_table_statement = text("""
        CREATE TABLE IF NOT EXISTS entity_urls (
            entity VARCHAR(255) NOT NULL,
            url VARCHAR(255) NOT NULL,
            country VARCHAR(255) NOT NULL,
            batch_id VARCHAR(255) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            cloud_storage_uri VARCHAR(255),
            PRIMARY KEY (entity, country),
            INDEX idx_entity (entity)
        );
    """)

    try:
        with engine.begin() as connection:  # Use `begin()` for automatic commit/rollback
            connection.execute(create_table_statement)
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
    insert_stmt = text("""
        INSERT INTO entity_urls (entity, url, country, batch_id, created_at, cloud_storage_uri)
        VALUES (:entity, :url, :country, :batch_id, :created_at, :cloud_storage_uri)
    """)
    try:
        with engine.begin() as connection:  # Use `begin()` for automatic commit/rollback
            connection.execute(insert_stmt, **entity_url_data)
            logger.info("New entity_url entry inserted successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Failed to insert entity_url entry: {e}")
        raise