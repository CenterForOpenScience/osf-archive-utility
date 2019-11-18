import os
import argparse
import json
import logging
import time
import requests
import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument(
    '-g',
    '--guid',
    help='This is the GUID of the target node on the OSF'
)
parser.add_argument(
    '-d',
    '--directory',
    help='This is the target Directory for the project and its files'
)

parser.add_argument(
    '-p',
    '--pagesize',
    help='How many logs should appear per file? Default is 100'
)

parser.add_argument(
    '-t',
    '--token',
    help='Auth token for osf. This is required.'
)


def json_with_pagination(path, guid, page, url, token):
    # Get JSON of registration logs
    response = make_json_api_request(url, token)

    # Craft filename based on page number
    json_filename = guid + '-' + str(page) + '.json'
    file_location = os.path.join(path, json_filename)
    json_data = response['data']
    with open(file_location, 'w') as file:
        json.dump(json_data, file)
    return response


def make_json_api_request(url, token):
    auth_header = {'Authorizataion': f'Bearer {token}'}
    keep_trying = True
    response = None

    while keep_trying:
        keep_trying = False
        try:
            response = requests.get(url, headers=auth_header)
            if response.status_code == 429:
                keep_trying = True
                response_headers = response.headers
                wait_time = response_headers['Retry-After']
                logging.log(logging.INFO, 'Throttled: retrying in {wait_time}s')
                time.sleep(int(wait_time))
            elif response.status_code >= 400:
                status_code = response.status_code
                content = getattr(response, 'content', None)
                raise requests.exceptions.HTTPError(
                    'Status code {}. {}'.format(status_code, content))
        except requests.exceptions.RequestException as e:
            logging.log(logging.ERROR, 'HTTP Request failed: {}'.format(e))
            raise
    try:
        return response.json()
    except json.decoder.JSONDecodeError:
        return None


def main():
    # Arg Parsing
    args = parser.parse_args()
    guid = args.guid
    directory = args.directory
    pagesize = args.pagesize
    bearer_token = args.token

    # Args handling
    if not guid:
        raise ValueError('Project GUID must be specified! Use -g')
    if not bearer_token:
        raise ValueError('Token must be specified! Use -t')
    if not directory:
        # Setting default to current directory
        directory = '.'
    if not pagesize:
        pagesize = 100

    create_logs(guid, directory, pagesize, bearer_token)


def create_logs(guid, directory, pagesize, bearer_token, base_url=None, logs_url=None):
    if not base_url:
        base_url = settings.OSF_API_URL
    if not logs_url:
        logs_url = settings.OSF_LOGS_URL

    # Creating directories
    path = os.path.join(directory, guid)
    if not os.path.exists(path):
        os.mkdir(path)
    path = os.path.join(path, 'logs')
    try:
        os.mkdir(path)
    except FileExistsError:
        pass

    # Retrieving page 1
    url = base_url + logs_url.format(guid, pagesize)
    response = json_with_pagination(path, guid, 1, url, bearer_token)
    page_num = 2

    # Retrieve the rest of the pages (if applicable)
    while response['links']['next']:
        next_link = response['links']['next']
        response = json_with_pagination(path, guid, page_num, next_link, bearer_token)
        page_num = page_num + 1

    print('Log data successfully transferred!')


if __name__ == '__main__':
    main()
