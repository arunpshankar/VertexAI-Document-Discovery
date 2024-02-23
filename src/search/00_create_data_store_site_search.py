import subprocess
import requests
import json


PROJECT_ID = 'arun-genai-bb'
DATA_STORE_ID = '2222'



def set_access_token() -> str:
        """
        Fetch an access token for authentication.

        Returns:
        - str: The fetched access token.
        """
        
        cmd = ["gcloud", "auth", "print-access-token"]
        try:
            token = subprocess.check_output(cmd).decode('utf-8').strip()
            
            return token
        except subprocess.CalledProcessError as e:
              print(e)
            

url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{PROJECT_ID}/locations/global/collections/default_collection/dataStores?dataStoreId={DATA_STORE_ID}"
headers = {
        "Authorization": f"Bearer {set_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": PROJECT_ID
    }



data = {
      'displayName': 'spider_man',
      'industryVertical': 'GENERIC',
      'solutionTypes': ['SOLUTION_TYPE_SEARCH'],
      'contentConfig': 'PUBLIC_WEBSITE',
      'searchTier': 'STANDARD',
      'searchAddOns': ['LLM']
  }



#data = {'targetSite': '*.hsbc.com/*', 'type': 'INCLUDE', 'exactMatch': True}

response = requests.post(url, headers=headers, json=data)
    


print(response.json())





