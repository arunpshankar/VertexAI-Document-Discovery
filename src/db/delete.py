from src.utils.db import create_engine_with_connection_pool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine
from src.config.logging import logger
from src.config.setup import config
from sqlalchemy import text


engine = create_engine_with_connection_pool()


def delete_table() -> None:
    """
    Deletes a table from the database.

    Args:
    table_name (str): The name of the table to delete.
    """
    # SQL command to delete a table
    sql_command = text(f"DROP TABLE IF EXISTS {config.CLOUD_SQL_TABLE}")
    
    try:
        # Use the engine to execute the SQL command
        with engine.connect() as connection:
            connection.execute(sql_command)
            logger.info(f"Table {config.CLOUD_SQL_TABLE} deleted successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error occurred while trying to delete table {config.CLOUD_SQL_TABLE}: {e}")
