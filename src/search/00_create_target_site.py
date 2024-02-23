import subprocess
import requests
import json


PROJECT_ID = 'arun-genai-bb'
DATA_STORE_ID = '2345'



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
            

url = f"https://discoveryengine.googleapis.com/v1alpha/projects/{PROJECT_ID}/locations/global/collections/default_collection/dataStores/{DATA_STORE_ID}/siteSearchEngine/targetSites"
headers = {
        "Authorization": f"Bearer {set_access_token()}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": PROJECT_ID
    }



  #-H "X-GFE-SSL: yes" -H "X-Goog-User-Project: gdc-ai-playground"    "https://discoveryengine.googleapis.com/v1alpha/projects/gdc-ai-playground/locations/global/dataStores/viqas-ui-test/siteSearchEngine/targetSites" -d '{ "providedUriPattern": "en.wikipedia.org/wiki/Cereal",  "type": "INCLUDE"}'


data = {'providedUriPattern': '*.sbi.in/*', 'type': 'INCLUDE', 'exactMatch': True}
#data = {'targetSite': '*.hsbc.com/*', 'type': 'INCLUDE', 'exactMatch': True}

response = requests.post(url, headers=headers, json=data)
    


print(response.json())





