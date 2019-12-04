import os
import asyncio
import argparse
from settings import OSF_API_URL
from IA.utils import (
    get_with_retry,
    get_paginated_data,
)

HERE = os.path.dirname(os.path.abspath(__file__))


async def write_wiki_content(page):
    resp = get_with_retry(page['links']['download'], retry_on=(429,))
    file_path = os.path.join(HERE, destination, guid, f'{page["attributes"]["name"]}.md')
    with open(file_path, 'wb') as fp:
        fp.write(resp.content)


async def main(guid, destination=''):
    """
    Usually asynchronous requests/writes are reserved for times when it's truely necessary, but
    given the fact that we have like 4 days left in the sprint and this going to be the first
    py3 thing in the repo, I've decided to whip out the big guns and concurrently gather all
    wiki pages simultaneously (except for the first one) and then stream them all to local
    files simultaneously just because it's easy to do with py3 and will save a nano-second or two.

    :param guid:
    :return:
    """

    url = f'{OSF_API_URL}v2/registrations/{guid}/wikis/'

    pages_as_list = await get_paginated_data(url)
    write_tasks = []
    for page in pages_as_list:
        write_tasks.append(write_wiki_content(page, destination, guid))

    await asyncio.gather(*write_tasks)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g',
        '--guid',
        help='The guid of the registration of who\'s wiki you want to dump.',
        required=True
    )
    args = parser.parse_args()

    guid = args.guid
    asyncio.run(main(guid))
