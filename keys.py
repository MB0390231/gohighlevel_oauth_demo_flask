import os
from dotenv import load_dotenv

# Load environment variables from .env file into os.environ
load_dotenv()


class Config(object):
    pass


class GoogConfig(Config):
    """Google API Configurations"""

    MDS_SHEET_ID = os.environ.get("MDS_SHEET_ID")
    TYPE = os.environ.get("type")
    PROJECT_ID = os.environ.get("project_id")
    PRIVATE_KEY_ID = os.environ.get("private_key_id")
    PRIVATE_KEY = os.environ.get("private_key")
    CLIENT_EMAIL = os.environ.get("client_email")
    CLIENT_ID = os.environ.get("client_id")
    AUTH_URI = os.environ.get("auth_uri")
    TOKEN_URI = os.environ.get("token_uri")
    AUTH_PROVIDER_X509_CERT_URL = os.environ.get("auth_provider_x509_cert_url")
    CLIENT_X509_CERT_URL = os.environ.get("client_x509_cert_url")
    UNIVERSE_DOMAIN = os.environ.get("universe_domain")
    CREDENTIALS = {
        "type": TYPE,
        "project_id": PROJECT_ID,
        "private_key_id": PRIVATE_KEY_ID,
        "private_key": PRIVATE_KEY,
        "client_email": CLIENT_EMAIL,
        "client_id": CLIENT_ID,
        "auth_uri": AUTH_URI,
        "token_uri": TOKEN_URI,
        "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
        "client_x509_cert_url": CLIENT_X509_CERT_URL,
    }


class GoHighLevelConfig(Config):
    AGENCY_ACCESS_TOKEN = os.environ.get("AGENCY_ACCESS_TOKEN")
    BASE_URL = os.environ.get("BASE_URL")
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
