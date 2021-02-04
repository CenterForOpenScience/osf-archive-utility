
osf-pigeon
========================

A utility for archiving OSF data to archive.org  


Purpose
============

This utility takes a publicly available OSF registration guid as a parameter and using that info alone it will transfer
that registration, file data, metadata et all to Internet Archive.

Install
============

Simply install the package using python's package manager pip with bash:
 
```
pip3 install osf_pigeon
```

 
To use for local development just remember to install the developer requirements using:

```
pip3 install -r dev.txt
```


Use
============

This should be able to export registrations from 
Assuming the registration is fully public and the DOI has been minted properly at datacite. 


Run from package
============

Simply import the module and enter a guid with credentials::

```
 from osf_pigeon.pigeon import main

 main(
    'guid0',
    datacite_username='test_datacite_username',
    datacite_password='test_datacite_password',
    datacite_prefix='test_datacite_prefix',
    ia_access_key='test_datacite_password',
    ia_secret_key='test_datacite_password',
 )
```
That's it!


Sync Metadata without files
============

```
 from osf_pigeon.pigeon import sync_metadata

 sync_metadata(
    'guid0',
    ia_access_key='test_datacite_password',
    ia_secret_key='test_datacite_password',
    metadata={
        'title': 'This is the title',
        'anything': 'This is isn't validated'
    }
)
```

The IA bucket may take several minutes to reflect changes.

Run as script
============

To run as script just -m to execute the module:
```
 python3 -m osf_pigeon -g u8p3q 
```

Running in development
========================


Tests
============

Running tests are easy enough just::
```
 pip3 install -r dev.txt
 python3 -m pytest . 
```


Linting
============


Optional, but recommended: To set up pre-commit hooks (will run
formatters and linters on staged files)::

 pip install pre-commit
 pre-commit install --allow-missing-config