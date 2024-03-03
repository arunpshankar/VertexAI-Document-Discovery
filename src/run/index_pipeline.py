from src.batch.create import process_dataframe_chunks
from src.batch.create import load_dataframe
from src.utils.gcp import upload_to_gcs
from src.config.logging import logger 
from src.config.setup import config
from datetime import datetime 
import os 


if __name__ == '__main__':
    # process input csv, break it into chunks and weite to local
    try:
        df = load_dataframe(config.INPUT_FILE_PATH)
        process_dataframe_chunks(df, config.LOCAL_OUTPUT_PATH)
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")

    timestamp_folder = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    # copy the batched chunks from local to GCS 
    try:
        for filename in os.listdir(config.LOCAL_OUTPUT_PATH):
            source_file_path = os.path.join(config.LOCAL_OUTPUT_PATH, filename)
            destination_blob_name = f"{timestamp_folder}/{filename}"
            upload_to_gcs(config.BUCKET, source_file_path, destination_blob_name)
    except Exception as e:
        logger.error(f"Failed to upload files to GCS: {e}")

    
