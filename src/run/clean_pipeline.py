from src.search.delete import delete_data_store
from src.search.delete import list_data_stores
from src.search.delete import delete_app
from src.search.delete import list_apps
from src.db.delete import delete_table
from src.utils.gcp import flush_bucket
from src.config.setup import config
from src.config.setup import logger


def delete_search_apps(prefix: str) -> None:
    """
    Deletes search apps that have a display name starting with the specified prefix.

    Args:
    - prefix (str): The prefix to filter search apps by.

    Returns:
    - None
    """
    try:
        engines = list_apps()
        for engine in engines:
            display_name = engine.get('displayName', '')
            if display_name.startswith(prefix):
                logger.info(f"Deleting app: {display_name}")
                name = engine.get('name', '')
                delete_app(name)
    except Exception as e:
        logger.error(f"Error deleting search apps: {e}")


def delete_data_stores(prefix: str) -> None:
    """
    Deletes data stores that have a display name starting with the specified prefix.

    Args:
    - prefix (str): The prefix to filter data stores by.

    Returns:
    - None
    """
    try:
        data_stores = list_data_stores()
        for data_store in data_stores:
            display_name = data_store.get('displayName', '')
            if display_name.startswith(prefix):
                logger.info(f"Deleting data store: {display_name}")
                data_store_name = data_store.get('name', '')
                delete_data_store(data_store_name)
    except Exception as e:
        logger.error(f"Error deleting data stores: {e}")


def main():
    """
    Main function to orchestrate the cleanup of tables, search apps, and data stores.

    Returns:
    - None
    """
    # Clean Cloud SQL table 
    delete_table()

    # Clean search apps and data stores with a specific prefix
    prefix = "site_search"
    delete_search_apps(prefix)
    delete_data_stores(prefix)

    # Clean GCS bucket 
    flush_bucket(config.BUCKET)
    

if __name__ == "__main__":
    main()
