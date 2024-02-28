from src.config.logging import logger
from src.config.setup import config
from typing import Optional
from typing import Dict
from typing import List
from typing import Any
import subprocess
import requests


def create_search_app(data_store_id) -> Optional[Dict[str, Any]]:
    """
    Creates a site search application using the Google Discovery Engine API.

    This function constructs a POST request to the Google Discovery Engine API to create a
    new site search app for a list of 50 company urls webpages 

    Returns:
        dict: A dictionary containing the response data from the API if the request is successful.
        None: If the request fails.
    """
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/engines?engineId={data_store_id}"

    # Headers for the request
    headers = {
        "Authorization": f"Bearer {config.ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": config.PROJECT_ID
    }

    # Request payload
    data = {
        "displayName": f'moodys_site_search_{data_store_id}',
        "dataStoreIds": [data_store_id],
        "solutionType": "SOLUTION_TYPE_SEARCH", 
        "searchEngineConfig": {
            "searchTier": "SEARCH_TIER_ENTERPRISE",
            
        }
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        logger.info(f"Site search app created successfully for batch {data_store_id}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create document search app: {str(e)}")
        return None
    

def fetch_access_token() -> Optional[str]:
    """
    Fetch an access token for authentication.
    Returns:
        Optional[str]: The fetched access token if successful, None otherwise.
    """
    cmd = ["gcloud", "auth", "print-access-token"]
    try:
        token = subprocess.check_output(cmd).decode('utf-8').strip()
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch access token: {e}")
        return None


def create_headers() -> Dict[str, str]:
    """
    Create headers for the HTTP request including authorization.
    Returns:
        Dict[str, str]: Headers for the request.
    """
    token = fetch_access_token()
    if token is None:
        raise RuntimeError("Failed to obtain access token")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": config.PROJECT_ID
    }


def create_request_body(uri_patterns: List[str], data_store_id: str) -> Dict[str, Any]:
    """
    Create the request body for target sites batch creation.
    Args:
        uri_patterns (List[str]): A list of URI patterns to include in the target sites.
    Returns:
        Dict[str, Any]: The constructed request body.
    """
    requests_list = [{
        'parent': f'projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores/{data_store_id}/siteSearchEngine',
        'targetSite': {'providedUriPattern': uri_pattern, 'type': 'INCLUDE', 'exactMatch': True}
    } for uri_pattern in uri_patterns]
    
    return {"requests": requests_list}


def extract_batch_id(filepath: str) -> str:
    """
    Extracts the batch ID from a filepath.
    
    Args:
    - filepath: The filepath in the format 'path/to/batch_XXXX_YYYY.jsonl'.
    
    Returns:
    - The batch ID in the format 'XXXX_YYYY'.
    """
    # Split the filepath by '/' and pick the last element
    filename = filepath.split('/')[-1]
    # Split the filename by '_', pick the parts with numbers, and join them back
    parts = filename.split('_')
    batch_id = '_'.join(parts[1:3]).replace('.jsonl', '')
    return batch_id


def create_data_store(batch_id):
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores?dataStoreId={batch_id}"
    headers = {
            "Authorization": f"Bearer {fetch_access_token()}",
            "Content-Type": "application/json",
            "X-Goog-User-Project": config.PROJECT_ID
        }
    data = {
        'displayName': f'moodys_site_search_{batch_id}',
        'industryVertical': 'GENERIC',
        'solutionTypes': ['SOLUTION_TYPE_SEARCH'],
        'contentConfig': 'PUBLIC_WEBSITE',
        'searchTier': 'STANDARD'
        
    }
    response = requests.post(url, headers=headers, json=data)
    return response


def post_target_sites(data: Dict[str, Any], data_store_id: str) -> Optional[Dict[str, Any]]:
    """
    Post target sites to the specified URL using the Google Cloud Discovery Engine API.
    Args:
        data (Dict[str, Any]): The data to be posted as JSON.
    Returns:
        Optional[Dict[str, Any]]: The JSON response if successful, None otherwise.
    """
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{config.PROJECT_ID}/locations/global/collections/default_collection/dataStores/{data_store_id}/siteSearchEngine/targetSites:batchCreate"
    headers = create_headers()
    
    
    response = requests.post(url, headers=headers, json=data)
    #response.raise_for_status()  # This will raise an HTTPError if the response was an error
    return response.json()


def chunk_data(data, size):
    """Yield successive size chunks from data."""
    for i in range(0, len(data), size):
        yield data[i:i + size]

