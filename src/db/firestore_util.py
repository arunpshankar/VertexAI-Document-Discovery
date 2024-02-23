
from src.config.logging import logger
from src.config.setup import config
from google.cloud import firestore
from typing import List
import pandas as pd
import os

# Initialize Firestore client
db = firestore.Client(project=config.PROJECT_ID)

def create_and_populate_collection(collection_name: str, csv_path: str) -> None:
    """
    Creates a Firestore collection and populates it with data from a CSV file.

    Parameters:
    - collection_name (str): The name of the Firestore collection to create.
    - csv_path (str): The path to the CSV file to import data from.
    """
    #try:
    # Load CSV data
    df = pd.read_csv(csv_path)
    
    # Convert DataFrame to dictionary and add to Firestore
    for _, row in df.iterrows():
        doc_ref = db.collection(collection_name).document()
        doc_ref.set(row.to_dict())
    
    print(f"Data from {csv_path} added to collection '{collection_name}'.")
   

def delete_collection(collection_name: str) -> None:
    """
    Deletes all documents within a Firestore collection.

    Parameters:
    - collection_name (str): The name of the Firestore collection to delete.
    """
    try:
        docs = db.collection(collection_name).stream()
        for doc in docs:
            doc.reference.delete()
        
        print(f"Collection '{collection_name}' and all its documents have been deleted.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set path to your CSV file
    csv_path = "./data/universities.csv"
    
    # Define your collection name
    collection_name = "universities"
    
    # Create and populate collection
    create_and_populate_collection(collection_name, csv_path)
    
    # Uncomment below line to delete the collection
    # delete_collection(collection_name)
