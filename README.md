
osf-pigeon
========================

A utility for archiving OSF data to archive.org  

Purpose
============

This is a mirco-service that takes an OSF registration and mirrors it's files and metadata at
Internet Archive.

Use
============

This should be able to export registration data from the OSF to archive.org assuming the registration is fully public 
and the DOI has been minted by the start of the archive job and the registration isn't withdrawn. 


Install/Run
============

Simply set your add your environment settings in `local.py` file 
and run with bash command:

```
    pip3 install -r requirements.txt
    python3 -m osf_python
```
That's it! Your OSF-Pigeon server should be up and running.

Running in development
========================

The osf.io and archive.org use a constellation of services for both testing and live environments here are
 recommended settings for each environment:
 
   - production
        - OSF_API_URL: https://api.osf.io/
        - OSF_FILES_URL: https://files.us.osf.io/
        - DATACITE_PREFIX: 10.17605
        - DATACITE_URL: https://mds.datacite.org/
        - DOI_FORMAT: {prefix}/osf.io/{guid}
        - OSF_COLLECTION_NAME: osf-registrations
        - ID_VERSION: v1
   - staging
        - OSF_API_URL: https://api.staging.osf.io/
        - OSF_FILES_URL: https://files.us.staging.osf.io/
        - DATACITE_PREFIX: 10.70102
        - DATACITE_URL: https://mds.test.datacite.org/
        - DOI_FORMAT: {prefix}/fk2osf.io/{guid}
        - OSF_COLLECTION_NAME: cos-dev-sandbox
        - ID_VERSION: staging_v1
   - local
        - OSF_API_URL: http://192.168.168.167:8000/
        - OSF_FILES_URL: http://192.168.168.167:7777/
        - DATACITE_PREFIX: 10.70102
        - DATACITE_URL: https://mds.test.datacite.org/
        - DOI_FORMAT: {prefix}/fk2osf.io/{guid}
        - OSF_COLLECTION_NAME: cos-dev-sandbox
        - ID_VERSION: local_v1

Setting an `OSF_BEARER_TOKEN` for a registration is not necessary for permissions, but is recommended to avoid 
rate limiting. Credentials for the Datacite and Internet Archive should be obtained via your institution.

Tests
============

Running tests are easy enough by just running the bash command:
```
 pip3 install -r dev.txt
 python3 -m pytest . 
```

Overview
================
When a registration is made public on the OSF the platform will begin to upload that registrations data and metadata to 
archive.org in order to save the registration for posterity. This involves uploading the registration's raw archived 
data to archive.org as well as supplementary JSON/XML metadata files describing that registration. To aid with searchability 
and we are also updating the metadata associated archive.org storage item to reflect the registration it corresponds to.  


Metadata Syncing Details
================

When a registration is made public on the OSF the platform will start sending syncing requests to Pigeon to sync 
metadata with it's Internet Archive item. 

There are two types of metadata being sent from OSF registrations, typical registration metadata which is set once on
creation and editable metadata which changes continually as registrations are edited. Here is the list of attributes 
that are synced with IA and their implementation details:

- Metadata set once on creation:
    - publisher
        - Should always be set to "Center for Open Science"
        - This is an IA recommended keyword.       
    - creator
        - These are the biblographic contributors for a registration.
        - This is an IA recommended keyword.
    - date
        - The date the registration was registered.
        - This is an IA recommended keyword.
    - osf_registry
        - This is the title of the Registration Provider for each registration.
        - IA recommended we add the `osf_` prefix to this to assert our brand. 
    - osf_registration_schema
        - This is the title of the registration schema used.
        - IA recommended we add the `osf_` prefix to this to assert our brand. 
    - osf_registration_doi
        - This is the DOI of the registration that has been archived, this is not a DOI reffering to any
        other published article or document, that is the `article_doi`
        - IA recommended we add the `osf_` prefix to this to assert our brand. 
    - source
        - A url to the OSF registration
        - This is an IA recommended keyword.
    - parent
        - A link to any parent registrations linked to that item
    - children
        - A link to any child registrations/components linked to that item

- Editable Metadata (synced continually)
    - title
        - This is the registration's title
    - description
        - This is the registration's description.
    - osf_category
        - This is the registration's category.  
        - IA recommended we add the `osf_` prefix to this to assert our brand. 
    - osf_subjects
        - These are a list the titles of a registration's subjects, usually the scientific discipline that registration.
        - IA recommended we add the `osf_` prefix to this to assert our brand. 
    - osf_tags
        - These are a list of tags to aid in the searchability of the registration. 
        - IA recommended we add the `osf_` prefix to this to assert our brand.
    - article_doi
        - This is a user created DOI that is supplemental to the registration, not a DOI created for that archived
         registration, that is the `osf_registration_doi`
    - license  
        - This is a url to the license for the registration if it has one.
    - affiliated_institutions
        - A list titles of the institutions affiliated with the registration.


JSON/XML Metadata Details
================

Each archived registration includes four json files and one xml file with metadata pertaining to the archived registration, these files
 are: 
 
 - registration.json
    - General metadata for the registration, title, description and links to all public relationships.
 - wiki.json
    - The text and metadata details associated with that registration's wiki.
 - contributors.json
    - The list of contributor to the registration including extra information about their ORCID identifiers and
     affiliated institutions.
  - logs.json
    - A list of all that registrations logs. 
  - datacite.xml
    - This contains the datacite's metadata for the DOI corresponding to that registration,
