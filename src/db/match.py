from src.utils.db import create_engine_with_connection_pool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.base import Engine 
from src.config.logging import logger
from src.config.setup import config
from sqlalchemy import text


engine = create_engine_with_connection_pool()


def find_entity_url_by_key(entity: str, country: str) -> dict:
    """
    Finds a row in the 'entity_urls' table based on the composite primary key (entity and country).

    Args:
        entity: The entity part of the composite primary key.
        country: The country part of the composite primary key.

    Returns:
        A dictionary representing the found row, or None if no matching row is found.
    """
    select_stmt = text(
        f"SELECT entity, url, country, batch_id, created_at, cloud_storage_uri FROM {config.CLOUD_SQL_TABLE} "
        "WHERE entity = :entity AND country = :country"
    )

    try:
        with engine.connect() as connection:
            result = connection.execute(select_stmt, {"entity": entity, "country": country}).fetchone()
            if result:
                logger.info(f"Matching row for {entity} in {country} found.")
                # Map the result to a dictionary using specified keys
                result_dict = {
                    "entity": result[0],
                    "url": result[1],
                    "country": result[2],
                    "batch_id": result[3],
                    "created_at": result[4],
                    "cloud_storage_uri": result[5]
                }
                return result_dict
            else:
                logger.info(f"No matching row for {entity} in {country}.")
                return None
    except SQLAlchemyError as e:
        logger.error(f"Failed to find entity_url entry: {e}")
        raise