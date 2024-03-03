from src.search.delete import delete_data_store
from src.search.delete import list_data_stores
from src.search.delete import delete_app
from src.search.delete import list_apps
from src.db.delete import delete_table
from src.config.setup import logger


# clean table
delete_table()

# clean data stores and search apps 
try:
    engines = list_apps()
    for engine in engines:
        display_name = engine.get('displayName', '')
        if display_name.startswith('moodys_site_search'):
            logger.info(f"Deleting app: {display_name}")
            name = engine.get('name', '')
            delete_app(name)

    data_stores = list_data_stores()
    for data_store in data_stores:
        display_name = data_store.get('displayName', '')
        if display_name.startswith('moodys_site_search'):
            logger.info(f"Deleting data store: {display_name}")
            data_store_name = data_store.get('name', '')
            delete_data_store(data_store_name)
except Exception as e:
    logger.error(f"Error during execution: {e}")

# clean gcs bucket 


