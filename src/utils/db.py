from google.cloud.sql.connector import Connector
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.base import Engine 
from src.config.logging import logger
from sqlalchemy import create_engine
from src.config.setup import config


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