import os

DATACITE_USERNAME = os.environ.get("DATACITE_USERNAME")
DATACITE_PASSWORD = os.environ.get("DATACITE_PASSWORD")
OSF_BEARER_TOKEN = os.environ.get("OSF_BEARER_TOKEN")

# New tokens can be found at https://archive.org/account/s3.php
IA_ACCESS_KEY = os.environ.get("IA_ACCESS_KEY")
IA_SECRET_KEY = os.environ.get("IA_SECRET_KEY")
OSF_API_URL = os.environ.get("OSF_API_URL")
OSF_FILES_URL = os.environ.get("OSF_FILES_URL")
DATACITE_PREFIX = os.environ.get("DATACITE_PREFIX")
DATACITE_URL = os.environ.get("DATACITE_URL")
DOI_FORMAT = os.environ.get("DOI_FORMAT")
OSF_COLLECTION_NAME = os.environ.get("OSF_COLLECTION_NAME")
ID_VERSION = os.environ.get("ID_VERSION")
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 1))
FILES_TIMEOUT = int(os.environ.get('FILES_TIMEOUT', 300))

REG_ID_TEMPLATE = f"osf-registrations-{{guid}}-{ID_VERSION}"
PROVIDER_ID_TEMPLATE = f"osf-registration-providers-{{provider_id}}-{ID_VERSION}"

PIGEON_TEMP_DIR = os.environ.get(
    "PIGEON_TEMP_DIR", None
)  # setting to None allows tempfile.py to decide

HOST = "0.0.0.0"
PORT = 2020

SENTRY_DSN = os.environ.get("SENTRY_DSN")
