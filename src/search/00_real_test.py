import subprocess
import requests
import logging
from typing import Optional, Dict, Any, List


"""


https://cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.collections.dataStores/create

enable advanced site search -

https://cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.dataStores.siteSearchEngine/enableAdvancedSiteSearch

add target sites -

https://cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.dataStores.siteSearchEngine.targetSites

create engine -

https://cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.collections.engines/create
"""

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = 'arun-genai-bb'
DATA_STORE_ID = '2222'

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
        logging.error(f"Failed to fetch access token: {e}")
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
        "X-Goog-User-Project": PROJECT_ID
    }

def create_request_body(uri_patterns: List[str]) -> Dict[str, Any]:
    """
    Create the request body for target sites batch creation.
    Args:
        uri_patterns (List[str]): A list of URI patterns to include in the target sites.
    Returns:
        Dict[str, Any]: The constructed request body.
    """
    requests_list = [{
        'parent': f'projects/{PROJECT_ID}/locations/global/collections/default_collection/dataStores/{DATA_STORE_ID}/siteSearchEngine',
        'targetSite': {'providedUriPattern': uri_pattern, 'type': 'INCLUDE', 'exactMatch': True}
    } for uri_pattern in uri_patterns]
    
    return {"requests": requests_list}

def post_target_sites(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Post target sites to the specified URL using the Google Cloud Discovery Engine API.
    Args:
        data (Dict[str, Any]): The data to be posted as JSON.
    Returns:
        Optional[Dict[str, Any]]: The JSON response if successful, None otherwise.
    """
    url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{PROJECT_ID}/locations/global/collections/default_collection/dataStores/{DATA_STORE_ID}/siteSearchEngine/targetSites:batchCreate"
    headers = create_headers()
    
    
    response = requests.post(url, headers=headers, json=data)
    #response.raise_for_status()  # This will raise an HTTPError if the response was an error
    return response.json()
    

def main():
    """
    Main function to execute the target sites batch creation.
    """
    uri_patterns = url_patterns = [
"*.barclays.com/*",
"*.group.bnpparibas/*",
"*.credit-suisse.com/*",
"*.db.com/*",
"*.ubs.com/*",
"*.rbc.com/*",
"*.td.com/*",
"*.scotiabank.com/*",
"*.ccb.com/*",
"*.abchina.com/*",
"*.bankofchina.com/*",
"*.icbc.com.cn/*",
"*.mufg.jp/*"
    
    ]

    data = create_request_body(uri_patterns)
    
    response = post_target_sites(data)
    if response is not None:
        logging.info(f"Successfully posted target sites: {response}")
    else:
        logging.error("Failed to post target sites.")

if __name__ == "__main__":
    main()
