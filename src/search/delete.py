from src.utils.access import create_headers
from src.config.logging import logger
from src.config.setup import config
from typing import List
from typing import Dict
from typing import Any
import requests


def list_apps() -> List[Dict[str, Any]]:
    """
    Lists all site search apps under the specified project.

    Returns:
        List[Dict[str, Any]]: A list of engines if successful, an empty list otherwise.
    """
    url = f"https://discoveryengine.googleapis.com/v1/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/engines"
    headers = create_headers()
    params = {'filter': 'solution_type=SOLUTION_TYPE_SEARCH'}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # Raises an HTTPError if the response was an error
        content = response.json()
        engines = content.get('engines', [])
        logger.info(f"Successfully listed {len(engines)} apps.")
        return engines
    except Exception as e:
        logger.error(f"Failed to list apps: {e}")
        return []


def delete_app(name: str) -> requests.Response:
    """
    Deletes a specified app by name.

    Parameters:
        name (str): The name of the app to delete.

    Returns:
        requests.Response: The response object from the delete request.
    """
    url = f"https://discoveryengine.googleapis.com/v1/{name}"
    headers = create_headers()
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully deleted app: {name}")
        return response
    except Exception as e:
        logger.error(f"Failed to delete app {name}: {e}")
        return response


def list_data_stores() -> List[Dict[str, Any]]:
    """
    Lists all data stores under the specified project.

    Returns:
        List[Dict[str, Any]]: A list of dataStores if successful, an empty list otherwise.
    """
    url = f"https://discoveryengine.googleapis.com/v1/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores"
    headers = create_headers()
    params = {'filter': 'solution_type:SOLUTION_TYPE_SEARCH'}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        content = response.json()
        data_stores = content.get('dataStores', [])
        logger.info(f"Successfully listed {len(data_stores)} data stores.")
        return data_stores
    except Exception as e:
        logger.error(f"Failed to list data stores: {e}")
        return []


def delete_data_store(name: str) -> requests.Response:
    """
    Deletes a specified data store by name.

    Parameters:
        name (str): The name of the data store to delete.

    Returns:
        requests.Response: The response object from the delete request.
    """
    url = f"https://discoveryengine.googleapis.com/v1/{name}"
    headers = create_headers()
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        logger.info(f"Successfully deleted data store: {name}")
        return response
    except Exception as e:
        logger.error(f"Failed to delete data store {name}: {e}")
        return response
    