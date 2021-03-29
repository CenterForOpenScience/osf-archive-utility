
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


Run
============

Simply install and run from commandline

```
    python3 -m osf_python --env=staging
```
That's it! 

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