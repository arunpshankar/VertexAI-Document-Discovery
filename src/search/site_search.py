from google.cloud import discoveryengine
from google.api_core.client_options import ClientOptions
from google.protobuf import json_format
from src.config.logging import logger 
from src.config.setup import config
from typing import Optional
from typing import List
from typing import Dict


LOCATION = "global" 

def search_data_store(search_query: str, data_store_id: str) -> Optional[discoveryengine.SearchResponse]:
    """
    Search the data store using Google Cloud's Discovery Engine API.

    Args:
        search_query (str): The search query string.

    Returns:
        discoveryengine.SearchResponse: The search response from the Discovery Engine API.
    """
    try:
        client_options = (
            ClientOptions(api_endpoint=f"{LOCATION}-discoveryengine.googleapis.com")
            if LOCATION != "global"
            else None
        )

        client = discoveryengine.SearchServiceClient(client_options=client_options)

        serving_config = client.serving_config_path(
            project=config.PROJECT_ID,
            location=LOCATION,
            data_store=data_store_id,
            serving_config="default_config",
        )

        content_search_spec = discoveryengine.SearchRequest.ContentSearchSpec(
            snippet_spec=discoveryengine.SearchRequest.ContentSearchSpec.SnippetSpec(
                return_snippet=True
            )
        )

        request = discoveryengine.SearchRequest(
            serving_config=serving_config,
            query=search_query,
            page_size=5,
            content_search_spec=content_search_spec,
            query_expansion_spec=discoveryengine.SearchRequest.QueryExpansionSpec(
                condition=discoveryengine.SearchRequest.QueryExpansionSpec.Condition.AUTO,
            ),
            spell_correction_spec=discoveryengine.SearchRequest.SpellCorrectionSpec(
                mode=discoveryengine.SearchRequest.SpellCorrectionSpec.Mode.AUTO
            ),
        )

        response = client.search(request)
        return response

    except Exception as e:
        logger.error(f"Error during data store search: {e}")
        return None

def extract_relevant_data(response: Optional[discoveryengine.SearchResponse]) -> List[Dict[str, str]]:
    """
    Extracts entity, title, snippet, and link from the search response.

    Args:
        response (discoveryengine.SearchResponse): The search response object from the Discovery Engine API.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing the extracted information.
    """
    extracted_data = []

    if response is None:
        logger.error("No response received to extract data.")
        return extracted_data

    for result in response.results:
        data = {
            "entity": "",
            "title": "",
            "snippet": "",
            "link": ""
        }

        # Convert protocol buffer message to JSON
        result_json = json_format.MessageToDict(result.document._pb)

        # Extracting fields from JSON
        struct_data = result_json.get('structData', {})
        derived_struct_data = result_json.get('derivedStructData', {})

        # Extracting company
        company = struct_data.get("entity")
        if company:
            data["entity"] = company

        # Extracting title
        title = derived_struct_data.get("title")
        if title:
            data["title"] = title

        # Extracting snippet
        snippets = derived_struct_data.get("snippets")
        if snippets:
                data["snippet"] = snippets[0]['snippet']

        # Extracting link
        link = derived_struct_data.get("link")
        if link:
            data["link"] = link

        extracted_data.append(data)

    return extracted_data

# Usage example
if __name__ == "__main__":
    search_query = "annual report"

    try:
        results = search_data_store(search_query)
        if results:
            extracted_data = extract_relevant_data(results)
            for data in extracted_data:
                logger.info(f"Company: {data['company']}, Title: {data['title']}, Snippet: {data['snippet']}, Link: {data['link']}")
        else:
            logger.error("No results returned from search_data_store function.")
    except Exception as e:
        logger.error(f"Error executing search_data_store: {e}")
