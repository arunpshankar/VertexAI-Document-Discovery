from src.db.match import find_entity_url_by_key
from src.config.logging import logger 
from src.config.setup import * 
from src.search.site_search import search_data_store
from src.search.site_search import extract_relevant_data


if __name__ == '__main__':
    entity = 'Brown University'
    country = 'United States'
    query = f'{entity} {country} MS Computer Science filetype:pdf'
    # Match against SQL db 
    row = find_entity_url_by_key(entity, country)
    # get the batch_id 
    batch_id = row['batch_id']
    site_url = row['url']

    
    # construct the API call with the targeted query 
    response = search_data_store(query, batch_id)
    print(response)

    info = extract_relevant_data(response)
    print(info)




    # make the API call 


    # display the top 3 results 
     



