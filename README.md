# Intelligent Document Discovery with Vertex AI Search 🚀

This repository encapsulates a Python pipeline designed to simplify the process of batching URLs for creating search applications that leverage Google Cloud's Vertex AI Search. Follow the steps below to set up a local development environment, configure necessary dependencies, and jumpstart your journey into intelligent document discovery.

## Prerequisites 📋

Before diving in, make sure you have:

- Python 3.6 or later
- Git

You'll also need a Google Cloud Platform account with a project set up and the Vertex AI API enabled. Ensure you have permissions to create service accounts and manage API keys.

## Installation

Follow these steps to set up your local development environment for the Vertex AI Document Discovery pipeline.

### Clone the Repository 📂

1. **Get the Code**: Clone this repository to your local machine using the following command in your terminal:

   ```bash
   git clone https://github.com/arunpshankar/VertexAI-Document-Discovery.git
   cd VertexAI-Document-Discovery
   ```

### Set Up Your Environment 🛠️

2. **Create a Virtual Environment**: Isolate your project dependencies by creating a Python virtual environment:

   - **For macOS/Linux**:

     ```bash
     python3 -m venv .VertexAI-Document-Discovery
     source .VertexAI-Document-Discovery/bin/activate
     ```

   - **For Windows**:

     ```bash
     python3 -m venv .VertexAI-Document-Discovery
     .VertexAI-Document-Discovery\Scripts\activate
     ```

3. **Upgrade pip and Install Dependencies**: Ensure you're using the latest version of pip and install project dependencies:

   ```bash
   python3 -m pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Update Your PYTHONPATH**:

   Ensure your Python interpreter recognizes the project directory as a location for modules. 

   - **For macOS/Linux**:

     ```bash
     export PYTHONPATH=$PYTHONPATH:.
     ```

   - **For Windows** (use `set` instead of `export`):

     ```bash
     set PYTHONPATH=%PYTHONPATH%;.
     ```

### Configure Service Account Credentials 🔑

5. **Prepare Credentials Storage**: Create a directory within the project to securely store your Google Cloud service account key:

   ```bash
   mkdir credentials
   ```

6. **Generate and Store the Service Account Key**:

   - Navigate to the Google Cloud Console, access the "IAM & Admin" section, and manage "Service Accounts."
   - Select or create a service account, then add a new JSON key under the "Keys" tab.
   - Download the JSON file and move it to the `credentials` directory, renaming it to `key.json`.

Now, your environment is ready for intelligent document discovery with Vertex AI Search. Let's now proceed with configuring your Google Cloud project settings, enabling APIs, and running the provided Python scripts to automate your search applications. 

Next wewill see how to setup and exacrute workflows for creating, routing, and managing site URL indexes using Vertex AI Search and Google Cloud Platform (GCP) services such as Cloud SQL and Cloud Storage. 

The architecture incorporates two primary workflows: index creation and query routing, optimized for handling a large number of site URLs by partitioning them into manageable chunks.

### **Prerequisites**

Before you begin, ensure the following requirements are met:
- The Service Account Key is downloaded and stored in the `credentials` folder, renamed to `key.json`.
- The Cloud SQL Admin API is enabled within your GCP project.
- The Cloud SQL Client role has been granted to the IAM principal.
- Configuration details such as database credentials are populated in `config.yml` under the `/config` directory.

### **Workflow 1: Index Creation**

The process starts with a user input file containing site URLs, entity information, country metadata, etc. This file is partitioned into chunks, with each containing a maximum of 50 URLs (the current limit imposed by Vertex AI Search for a single datastore or site search application).

#### Steps for Index Creation:

1. Partition the input file into multiple files, each with 50 URLs, and upload them to Cloud Storage.
2. For each chunk, push the associated entity, site URL, and other information to a Cloud SQL table, mapping each entity to its batch number (chunk number).
3. Utilize the Vertex AI Search API to create a datastore and a search application for each chunk, identifying each datastore by its batch ID, thereby creating a scalable architecture capable of managing a large number of site URLs efficiently.

### **Workflow 2: Query Routing**

This workflow describes how to identify the appropriate batch ID for a user query based on the entity information it contains and route the query to the corresponding datastore.

#### Steps for Query Routing:

1. User submits a query including entity information and other details.
2. Query the Cloud SQL table using the entity information to find the corresponding row that contains the batch ID.
3. Use the batch ID to direct the API request to the relevant datastore, along with the user's query, significantly optimizing the search process by targeting only the datastore that contains the matching site URL.

### **Setting Up Your Environment**

- **Enable Cloud SQL Admin API**: Run `gcloud services enable sqladmin.googleapis.com` in your terminal.
- **Create a Database**: Execute `gcloud sql databases create <db name> --instance=<instance_name>` to create a new database within your Cloud SQL instance. Alternatively, you can create one via the GCP console for ease.

### **Running the Modules**

Within the `src/run` directory, you will find three modules:

- `index_pipeline.py`: Handles the chunking of URLs, uploads to GCS, creates datastores and search apps, and updates the Cloud SQL table with batch information for later query routing.
- `query_pipeline.py`: Use this module to test query routing functionality based on the given query.
- `clean_pipeline.py`: Cleans up resources by removing objects from the Cloud Storage bucket, entries from the Cloud SQL table, and deleting datastores and search apps from Vertex AI Search.

### **Configuration and Execution**

Ensure you have updated `config.yml` with relevant database details such as username, password, database name, and table name before executing the modules. Follow the outlined steps to create indexes and route queries efficiently, leveraging GCP's powerful cloud capabilities for your website's search functionality.

---

Happy coding! 🚀