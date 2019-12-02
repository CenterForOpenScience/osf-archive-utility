import requests
import os
import argparse
import time
import logging
import settings
from IA.utils import get_with_retry
from zipfile import ZipFile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HERE = os.path.dirname(os.path.abspath(__file__))


def consume_files(guid, token, directory):
    path = os.path.join(HERE, directory, guid)
    zip_url = f'{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip='

    try:
        os.mkdir(path)
    except FileExistsError:
        pass

    path = os.path.join(path, 'files')

    try:
        os.mkdir(path)
    except FileExistsError:
        pass

    if token:
        auth_header = {'Authorization': 'Bearer {}'.format(token)}
    else:
        auth_header = {}

    response = get_with_retry(zip_url.format(guid), retry_on=(429,), headers=auth_header)
    if response.status_code >= 400:
        status_code = response.status_code
        content = getattr(response, 'content', None)
        raise requests.exceptions.HTTPError('Status code {}. {}'.format(status_code, content))

    zipfile_location = os.path.join(path, f'{guid}.zip')
    with open(zipfile_location, 'wb') as file:
        file.write(response.content)

    with ZipFile(zipfile_location, 'r') as zipObj:
        zipObj.extractall(path)

    os.remove(zipfile_location)
    print('File data successfully transferred!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g',
        '--guid',
        help='This is the GUID of the target node on the OSF',
        required=True
    )
    parser.add_argument(
        '-t',
        '--token',
        help='This is the bearer token for auth. This is required',
        required=True
    )
    parser.add_argument(
        '-d',
        '--directory',
        help='This is the target Directory for the project and its files',
        default='.'
    )
    args = parser.parse_args()
    guid = args.guid
    directory = args.directory
    token = args.token

    consume_files(guid, token, directory)
