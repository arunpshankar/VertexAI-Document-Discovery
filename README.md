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


Happy coding! 🚀