from google.cloud.sql.connector import Connector
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine
from src.config.logging import logger
from src.config.setup import config
import sqlalchemy


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


if __name__ == "__main__":
    try:
        engine = create_engine_with_connection_pool()
        create_university_urls_table(engine)
        # Inserting data into university_urls table
        stanford_university_entry = {
            "university": "Stanford University",
            "url": "https://stanford.edu",
            "country": "USA",
            "batch_id": "1",
            "data_store_name": "Academic Records",
            "data_store_id": "DS-002",
            "app_name": "Campus Navigator",
            "app_id": "APP-002",
            "created_at": "2024-02-26 00:00:00",
            "index_status": "Indexed",
            "cloud_storage_uri": "gs://university_bucket/stanford_path"
        }
        insert_university_url(engine, stanford_university_entry)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
