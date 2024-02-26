from src.config.logging import logger  
from src.config.setup import * 
import pandas as pd
import os
import json  # Import the json module to serialize dict objects


def load_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load a DataFrame from a CSV file.

    Parameters:
    - file_path (str): The path to the CSV file.

    Returns:
    - pd.DataFrame: The loaded DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"DataFrame loaded successfully from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to load DataFrame from {file_path}: {e}")
        raise


def save_chunk_rows_as_jsonl(df_chunk: pd.DataFrame, filename: str) -> None:
    """
    Save rows from DataFrame chunk to a JSON Lines file.

    Parameters:
    - df_chunk (pd.DataFrame): The DataFrame chunk containing the rows.
    - filename (str): The output filename to save the rows in JSON Lines format.

    Returns:
    - None
    """
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            for _, row in df_chunk.iterrows():
                file.write(json.dumps(row.to_dict(), ensure_ascii=False) + '\n')  # Serialize row to JSON and write
        logger.info(f"Rows successfully saved to {filename} in JSON Lines format")
    except Exception as e:
        logger.error(f"Failed to save chunk rows as JSON Lines: {e}")


def process_dataframe_chunks(df: pd.DataFrame, output_dir: str, chunk_size: int = 50) -> None:
    """
    Process DataFrame in chunks, saving each chunk's rows to separate JSON Lines files.

    Parameters:
    - df (pd.DataFrame): The DataFrame to process.
    - output_dir (str): The directory where output files will be saved.
    - chunk_size (int, optional): The number of rows per chunk. Default is 50.

    Returns:
    - None
    """
    os.makedirs(output_dir, exist_ok=True)
    for start_row in range(0, df.shape[0], chunk_size):
        df_chunk = df.iloc[start_row:start_row + chunk_size]
        filename = f"{output_dir}/batch_{start_row + 1}_{start_row + df_chunk.shape[0]}.jsonl"
        save_chunk_rows_as_jsonl(df_chunk, filename)


if __name__ == '__main__':
    # Example usage
    try:
        data_file_path = './data/universities.csv'
        output_directory = './data/batch'
        df = load_dataframe(data_file_path)
        process_dataframe_chunks(df, output_directory)
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")