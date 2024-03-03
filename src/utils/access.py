from src.config.logging import logger
from src.config.setup import config
from typing import Optional
from typing import Dict
import subprocess


def fetch_access_token() -> Optional[str]:
    """
    Fetches an access token for authentication with Google Cloud services.

    Returns:
        Optional[str]: The fetched access token if successful, None otherwise.
    """
    cmd = ["gcloud", "auth", "print-access-token"]
    try:
        token = subprocess.check_output(cmd).decode('utf-8').strip()
        logger.info("Successfully fetched access token.")
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch access token: {e}")
        return None


def create_headers() -> Dict[str, str]:
    """
    Creates headers for HTTP requests, including authorization based on the access token.

    Returns:
        Dict[str, str]: Headers for the request.

    Raises:
        RuntimeError: If the access token cannot be obtained.
    """
    token = fetch_access_token()
    if token is None:
        logger.error("Failed to obtain access token.")
        raise RuntimeError("Failed to obtain access token")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Goog-User-Project": config.PROJECT_ID
    }
    return headers