from src.batch.create import process_dataframe_chunks
from src.batch.create import load_dataframe
from src.config.logging import logger 
from src.config.setup import *


if __name__ == '__main__':
    try:
        data_file_path = './data/universities.csv'
        output_directory = './data/batch'
        df = load_dataframe(data_file_path)
        process_dataframe_chunks(df, output_directory)
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")