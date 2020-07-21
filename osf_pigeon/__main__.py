import argparse
from osf_pigeon.pigeon import main


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g',
        '--guid',
        help='This is the GUID of the target node on the OSF',
        required=True
    )
    parser.add_argument(
        '-d',
        '--datacite_password',
        help='This is the password for using datacite\'s api',
        required=False
    )
    parser.add_argument(
        '-u',
        '--datacite_username',
        help='This is the username for using datacite\'s api',
        required=False
    )
    parser.add_argument(
        '-a',
        '--ia_access_key',
        help='This is the access key for using Internet Archive\'s api',
        required=False
    )
    parser.add_argument(
        '-s',
        '--ia_secret_key',
        help='This is the secret key for using Internet Archive\'s api',
        required=False
    )
    args = parser.parse_args()
    guid = args.guid
    datacite_password = args.datacite_password
    datacite_username = args.datacite_username
    ia_access_key = args.ia_access_key
    ia_secret_key = args.ia_secret_key
    main(
        guid,
        datacite_password=datacite_password,
        datacite_username=datacite_username,
        ia_access_key=ia_access_key,
        ia_secret_key=ia_secret_key
    )
