
from src.utils.db import create_engine_with_connection_pool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine 
from src.config.logging import logger
from src.config.setup import config
from sqlalchemy import text


engine = create_engine_with_connection_pool()


def create_table(engine: Engine):
    """
    Creates the 'entity_urls' table in the database if it doesn't exist.

    Args:
        engine: A SQLAlchemy engine object.
    """
    create_table_statement = text(f"""
        CREATE TABLE IF NOT EXISTS {config.CLOUD_SQL_TABLE} (
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
    insert_stmt = text(
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