from typing import Optional, List, Dict, Any
import requests
import json
import subprocess
from src.config.logging import logger
from src.config.setup import config

def fetch_access_token() -> Optional[str]:
    """
    Fetches an access token for authentication with Google Cloud services.

    Returns:
        Optional[str]: The fetched access token if successful, None otherwise.
    """
    cmd = ["gcloud", "auth", "print-access-token"]
    try:
        token = subprocess.check_output(cmd).decode('utf-8').strip()
        logger.info("Successfully fetched access token.")
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch access token: {e}")
        return None

def create_headers() -> Dict[str, str]:
    """
    Creates headers for HTTP requests, including authorization based on the access token.

    Returns:
        Dict[str, str]: Headers for the request.

    Raises:
        RuntimeError: If the access token cannot be obtained.
    """
    token = fetch_access_token()
    if token is None:
        logger.error("Failed to obtain access token.")
        raise RuntimeError("Failed to obtain access token")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": config.PROJECT_ID
    }
    return headers

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

if __name__ == '__main__':
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
