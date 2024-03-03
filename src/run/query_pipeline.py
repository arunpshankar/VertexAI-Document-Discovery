from src.db.match import find_entity_url_by_key
from src.config.logging import logger 
from src.config.setup import * 


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


    # make the API call 


    # display the top 3 results 
     



