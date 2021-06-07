"""
Values for our CI only, local testing is done by overriding local.py
"""

OSF_API_URL = "http://192.168.168.167:8000/"
OSF_FILES_URL = "http://192.168.168.167:7777/"
DATACITE_PREFIX = "10.70102"
DATACITE_URL = "https://mds.test.datacite.org/"
DOI_FORMAT = "{prefix}/fk2osf.io/{guid}"
OSF_COLLECTION_NAME = "cos-dev-sandbox"
ID_VERSION = "test_v1"
DATACITE_USERNAME = "test-datacite-username"
DATACITE_PASSWORD = "test-datacite-password"

IA_ACCESS_KEY = "Clyde Simmons is underrated"
IA_SECRET_KEY = "Ben Simmons is overrated"
OSF_BEARER_TOKEN = "Temple U is rated"

REG_ID_TEMPLATE = f"osf-registrations-{{guid}}-{ID_VERSION}"
PROVIDER_ID_TEMPLATE = f"osf-registration-providers-{{provider_id}}-{ID_VERSION}"
