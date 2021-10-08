import logging
import logging.handlers
import os
import sys

from googleapiclient.discovery import build, Resource
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

# Based on the https://developers.google.com/drive/api/v3/quickstart/python
from constants import LOGGER_DIR_PATH, LOGGER_MAX_FILE_SIZE, LOGGER_MAX_BACKUP_SIZE

CREDENTIALS_PATH_ENV = "CREDENTIALS_PATH"
TOKEN_PATH_ENV = "TOKEN_PATH"
SERVICE_ACCOUNT_PATH_ENV = "SERVICE_ACCOUNT_PATH"


class BaseGoogleService:

    scopes: list[str]
    service_name: str
    version: str
    service: Resource
    logger: logging.Logger

    def __init__(self, scopes: list, service_name: str, version: str, use_service_account: bool = False):
        self.scopes = scopes
        self.service_name = service_name
        self.version = version
        self.use_service_account = use_service_account

        self.init_logger()
        self.build_service()

    def init_logger(self):
        logger_name = getattr(self, 'name', __name__)
        self.logger = logging.Logger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        logger_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # set up console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logger_format)
        self.logger.addHandler(console_handler)
        # set up file handler to store logs - separate log file for each trader
        fh = logging.handlers.RotatingFileHandler(
            filename=os.path.join(LOGGER_DIR_PATH, f"{logger_name}.log"),
            maxBytes=LOGGER_MAX_FILE_SIZE, backupCount=LOGGER_MAX_BACKUP_SIZE
        )
        fh.setFormatter(logger_format)
        self.logger.addHandler(fh)

        self.logger.info("Logger was initiated successfully!")

    def build_service(self):
        """Method to build the Google service with credentials"""
        credentials = None
        if self.use_service_account:
            credentials = service_account.Credentials.from_service_account_file(
                os.getenv(SERVICE_ACCOUNT_PATH_ENV),
                scopes=self.scopes
            )
        else:
            token_path = os.getenv(TOKEN_PATH_ENV)
            credentials_path = os.getenv(CREDENTIALS_PATH_ENV)
            # This file stores the user's access and refresh tokens,
            # and it's created when the user authorizes for the first time.
            if os.path.exists(token_path):
                credentials = Credentials.from_authorized_user_file(token_path, self.scopes)
                self.logger.info("Built credentials from existing authorized user file.")
            # If there are no valid credentials available, let the user log in.
            if not credentials or not credentials.valid:
                if credentials and credentials.expired and credentials.refresh_token:
                    self.logger.info("Requesting new credentials as exising credentials expired.")
                    credentials.refresh(Request())
                else:
                    self.logger.info("Requesting new credentials as no credentials exist.")
                    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, self.scopes)
                    credentials = flow.run_local_server(port=0)
                # save credentials for the next run
                with open(token_path, 'w') as token_fd:
                    token_fd.write(credentials.to_json())
                    self.logger.info("New credentials are writen to the file.")
        # Store the Google Service
        self.service = build(self.service_name, self.version, credentials=credentials)

        self.logger.info("Service was built successfully!")

    def quit(self):
        self.service.close()
