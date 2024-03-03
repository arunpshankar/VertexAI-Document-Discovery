from src.config.logging import logger
from typing import Dict
from typing import Any
import subprocess
import yaml
import os

class Config:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
            # The following line ensures that the __init__ method is only called once.
            cls._instance.__initialized = False
        return cls._instance
    
    def __init__(self, config_path: str = "./config/config.yml"):
        """
        Initialize the Config class.

        Args:
        - config_path (str): Path to the YAML configuration file.
        """
        if self.__initialized:
            return
        self.__initialized = True
        
        self.__config = self._load_config(config_path)
        self.PROJECT_ID = self.__config['project_id']
        self.CREDENTIALS_PATH = self.__config['credentials_json']
        self._set_google_credentials(self.CREDENTIALS_PATH)
        self.ACCESS_TOKEN = self._set_access_token()
        self.REGION = self.__config['region']
        self.INPUT_FILE_PATH = self.__config['input_file_path']
        self.LOCAL_OUTPUT_PATH = self.__config['local_output_path']
        self.BUCKET = self.__config['bucket']
        self.CLOUD_SQL_INSTANCE = self.__config['cloud_sql_instance']
        self.CLOUD_SQL_USERNAME = self.__config['cloud_sql_username']
        self.CLOUD_SQL_PASSWORD = self.__config['cloud_sql_password']
        self.CLOUD_SQL_DATABASE = self.__config['cloud_sql_database']


    @staticmethod
    def _load_config(config_path: str) -> Dict[str, Any]:
        """
        Load the YAML configuration from the given path.

        Args:
        - config_path (str): Path to the YAML configuration file.

        Returns:
        - dict: Loaded configuration data.
        """
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load the configuration file. Error: {e}")

    @staticmethod
    def _set_google_credentials(credentials_path: str) -> None:
        """
        Set the Google application credentials environment variable.

        Args:
        - credentials_path (str): Path to the Google credentials file.
        """
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    @staticmethod
    def _set_access_token() -> str:
        """
        Fetch an access token for authentication.

        Returns:
        - str: The fetched access token.
        """
        logger.info("Fetching access token...")
        cmd = ["gcloud", "auth", "print-access-token"]
        try:
            token = subprocess.check_output(cmd).decode('utf-8').strip()
            logger.info("Access token obtained successfully.")
            return token
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to fetch access token. Error: {e}")


config = Config()