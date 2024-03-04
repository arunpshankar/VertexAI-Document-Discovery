Below is a polished version of the instructions you've provided, formatted as a Markdown README for a GitHub repository. This README includes a brief introduction, prerequisites, installation steps, and a guide to setting up service account credentials, structured to be user-friendly and easy to follow.

---

# Intelligent Document Discovery with Vertex AI Search

This repository contains a Python pipeline designed to automate URL batching for creating search applications leveraging Google Cloud's Vertex AI Search. By following the steps outlined below, you will set up a local development environment, configure necessary dependencies, and prepare your system for intelligent document discovery.

## Prerequisites

Before you begin, ensure you have the following installed on your machine:

- Python 3.6 or later
- Git

Additionally, you will need to have a Google Cloud Platform account and a project set up with the Vertex AI API enabled. Ensure you have the necessary permissions to create service accounts and manage API keys.

## Installation

### Clone the Repository

Start by cloning this repository to your local machine. Open a terminal and run the following command:

```bash
git clone <REPOSITORY_URL>
cd <REPOSITORY_DIRECTORY>
```

Replace `<REPOSITORY_URL>` with the actual URL of this repository and `<REPOSITORY_DIRECTORY>` with the name of the folder created by the `git clone` command.

### Create a Virtual Environment

Within the repository directory, create a Python virtual environment to manage your project's dependencies separately from your global Python installation:

```bash
python3 -m venv .Vertex
```

Activate the virtual environment with the following command:

For macOS/Linux:

```bash
source .Vertex/bin/activate
```

For Windows:

```bash
.Vertex\Scripts\activate
```

### Upgrade pip and Install Dependencies

Ensure you have the latest version of pip installed:

```bash
python3 -m pip install --upgrade pip
```

### Configure Service Account Credentials

1. **Create a Credentials Folder:**

   Inside the repository directory, create a folder named `credentials` to store your Google Cloud service account key:

   ```bash
   mkdir credentials
   ```

2. **Generate a Service Account Key:**

   - Navigate to the [Google Cloud Console](https://console.cloud.google.com/).
   - Go to the "IAM & Admin" section, then select "Service Accounts."
   - Find the service account you wish to use or create a new one.
   - Once you've selected a service account, navigate to the "Keys" tab.
   - Click "Add Key" and select "JSON." A JSON file containing your key will be downloaded.

3. **Store the Service Account Key:**

   Move the downloaded JSON file into the `credentials` folder and rename it to `key.json`.

## Usage

After setting up your environment and configuring your credentials, you can start developing your search applications with Vertex AI Search. Refer to the project's documentation for specific usage instructions and examples.