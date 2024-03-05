from src.search.site_search import extract_relevant_data
from src.search.site_search import search_data_store
from src.db.match import find_entity_url_by_key
from requests.exceptions import ConnectionError
from requests.exceptions import HTTPError
from src.config.logging import logger 
from typing import Optional 
from pathlib import Path
from typing import List 
from typing import Dict 
from typing import Any 
from tqdm import tqdm
import pandas as pd
import requests
import time


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
        

def read_and_query_csv(file_path: str, n: Optional[int] = None) -> None:
    """
    Reads entities from a CSV file, constructs queries for each entity and country,
    and executes searches. The top result from each search is saved into a separate CSV file.

    Parameters:
    - file_path (str): The file path to the CSV containing entities and their URLs.
    - n (Optional[int]): The number of rows to process. If None, process all rows.

    Returns:
    None
    """
    try:
        df = pd.read_csv(file_path)
        if n is not None:
            df = df.head(n)
        
        results = []

        for _, row in df.iterrows():
            entity = row['entity']
            country = row['country']
            search_topic = 'Graduate Handbook'  # Adjusted search topic

            match_row = find_entity_url_by_key(entity, country)
            if match_row is None:
                logger.error(f"No matching entity found in the database for {entity}, {country}.")
                continue

            batch_id = match_row['batch_id']
            site_url = match_row['url']

            query = f'{entity} {country} {search_topic} filetype:pdf site:{site_url}'
            logger.info(f'Executing query: {query}')

            response = search_data_store(query, batch_id)
            matches = extract_relevant_data(response)  # Adjusted to use extract_relevant_data

            if matches:
                top_match = matches[0]  # Assuming the first match is the top match
                results.append({
                    'entity': entity,
                    'country': country,
                    'title': top_match['title'],
                    'pdf_url': top_match['link']
                })
            else:
                logger.warning(f"No results found for {entity} in {country}.")

        if results:
            results_df = pd.DataFrame(results)
            results_df.to_csv('./data/results.csv', index=False)
            logger.info("Results successfully saved to './data/results.csv'.")
        else:
            logger.info("No results to save.")
            
    except Exception as e:
        logger.error(f"An error occurred while processing: {e}")


def download_pdfs_from_csv(csv_path: str, save_dir: str) -> None:
    """
    Reads the results CSV file and downloads PDFs from the provided URLs, with retries and progress indication.

    Parameters:
    - csv_path (str): The path to the CSV file containing the results.
    - save_dir (str): The directory where PDFs should be saved.

    Returns:
    None
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Failed to read CSV file at {csv_path}: {e}")
        return

    Path(save_dir).mkdir(parents=True, exist_ok=True)

    # Setup tqdm progress bar
    pbar = tqdm(df.iterrows(), total=len(df), desc="Downloading PDFs")

    for _, row in pbar:
        pdf_url = row['pdf_url']
        entity = row.get('entity', 'default_entity').replace(' ', '_')
        filename = f"{entity}.pdf"
        filepath = Path(save_dir) / filename
        
        success = False
        retries = 3
        for attempt in range(1, retries + 1):
            try:
                response = requests.get(pdf_url, timeout=attempt * 5)  # Gradually increasing timeout
                response.raise_for_status()  # Raise an exception for HTTP errors
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                success = True
                break
            except (ConnectionError, HTTPError) as e:
                logger.error(f"Attempt {attempt} failed for {pdf_url}: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Unexpected error occurred for {pdf_url}: {e}")
                break  # Break on unknown errors

        if not success:
            logger.error(f"Failed to download PDF after {retries} attempts: {pdf_url}")


if __name__ == '__main__':
    entity = 'Brown University'
    country = 'United States'
    search_topic = 'Graduate Handbook'
    execute_search_and_log_results(entity, country, search_topic)
    
    # Test Bulk Queries 
    read_and_query_csv('./data/entities.csv', 25)
    download_pdfs_from_csv('./data/results.csv', './data/pdfs')