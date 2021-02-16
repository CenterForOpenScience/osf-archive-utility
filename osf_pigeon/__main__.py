import argparse
from osf_pigeon.pigeon import main

from osf_pigeon.settings import (
    DATACITE_PASSWORD,
    DATACITE_USERNAME,
    IA_ACCESS_KEY,
    IA_SECRET_KEY,
    OSF_BEARER_TOKEN
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-g",
        "--guid",
        help="This is the GUID of the target node on the OSF",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--datacite_password",
        help="This is the password for using datacite's api",
        required=False,
    )
    parser.add_argument(
        "-u",
        "--datacite_username",
        help="This is the username for using datacite's api",
        required=False,
    )
    parser.add_argument(
        "-a",
        "--ia_access_key",
        help="This is the access key for using Internet Archive's api",
        required=False,
    )
    parser.add_argument(
        "-s",
        "--ia_secret_key",
        help="This is the secret key for using Internet Archive's api",
        required=False,
    )
    parser.add_argument(
        "-t",
        "--osf_bearer_token",
        help="This is the osf bear token for using OSF's api",
        required=False,
    )
    args = parser.parse_args()
    guid = args.guid
    datacite_password = args.datacite_password
    datacite_username = args.datacite_username
    ia_access_key = args.ia_access_key
    ia_secret_key = args.ia_secret_key
    osf_bearer_token = args.osf_bearer_token
    main(
        guid,
        datacite_password=datacite_password or DATACITE_PASSWORD,
        datacite_username=datacite_username or DATACITE_USERNAME,
        ia_access_key=ia_access_key or IA_ACCESS_KEY,
        ia_secret_key=ia_secret_key or IA_SECRET_KEY,
        osf_bearer_token=osf_bearer_token
    )
