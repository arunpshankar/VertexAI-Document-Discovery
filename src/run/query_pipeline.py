from src.search.site_search import extract_relevant_data
from src.search.site_search import search_data_store
from src.db.match import find_entity_url_by_key
from src.config.logging import logger 
from typing import List 
from typing import Dict 
from typing import Any 


def execute_search_and_log_results(entity: str, country: str, search_topic: str) -> None:
    """
    Executes a search based on the specified entity and country, logs relevant data from the search results.
    The function constructs a query to search for documents of a specific filetype related to the entity in the specified country,
    then logs the title, snippet, and link of each match found.

    Parameters:
    - entity (str): The name of the entity to search for.
    - country (str): The country where the entity is located.
    - search_topic (str): Search topic specific keywords.

    Returns:
    None
    """
    try:
        # Match against SQL database
        row = find_entity_url_by_key(entity, country)
        if row is None:
            logger.error("No matching entity found in the database.")
            return
        
        batch_id = row['batch_id']
        site_url = row['url']
        query = f'{entity} {country} {search_topic} filetype:pdf site:{site_url}'
        logger.info(f'Query: {query}')

        # Construct the API call with the targeted query
        response = search_data_store(query, batch_id)
        if response is None:
            logger.error("Failed to retrieve search data.")
            return

        matches = extract_relevant_data(response)
        log_search_results(matches)
    except Exception as e:
        logger.error(f"An error occurred during the search process: {e}")


def log_search_results(matches: List[Dict[str, Any]]) -> None:
    """
    Logs the title, snippet, and link of each match found in the search results in a structured and visually appealing format.

    Parameters:
    - matches (List[Dict[str, Any]]): A list of matches, each a dictionary with title, snippet, and link keys.

    Returns:
    None
    """
    if not matches:
        logger.info("No matches found.")
        return
    
    for match in matches:
        logger.info(f"\n--------------------------------------------------\n"
                    f"Title: {match['title']}\n"
                    f"Snippet: {match['snippet']}\n"
                    f"Link: {match['link']}\n"
                    f"--------------------------------------------------\n")
        

if __name__ == '__main__':
    entity = 'Brown University'
    country = 'United States'
    search_topic = 'Graduate financial aid guide Computer Science'
    execute_search_and_log_results(entity, country, search_topic)