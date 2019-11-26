import os
import math
import asyncio
import argparse
from settings import OSF_API_URL
from IA.utils import get_with_retry

HERE = os.path.dirname(os.path.abspath(__file__))


class WikiDumpError(Exception):
    pass


async def get_wiki_pages(guid, page, result={}):
    url = f'{OSF_API_URL}v2/registrations/{guid}/wikis/?page={page}'
    resp = get_with_retry(url, retry_on=(429,))
    result[page] = resp.json()['data']
    return result


async def write_wiki_content(page):
    resp = get_with_retry(page['links']['download'], retry_on=(429,))

    with open(os.path.join(HERE, f'/{page["attributes"]["name"]}.md'), 'wb') as fp:
        fp.write(resp.content)


async def main(guid):
    """
    Usually asynchronous requests/writes are reserved for times when it's truely necessary, but
    given the fact that we have like 4 days left in the sprint and this going to be the first
    py3 thing in the repo, I've decided to whip out the big guns and concurrently gather all
    wiki pages simultaneously (except for the first one) and then stream them all to local
    files simultaneously just because it's easy to do with py3 and will save a nano-second or two.

    :param guid:
    :return:
    """

    url = f'{OSF_API_URL}v2/registrations/{guid}/wikis/?page=1'
    tasks = []

    data = get_with_retry(url, retry_on=(429,)).json()
    result = {1: data['data']}

    if data['links']['next'] is not None:
        pages = math.ceil(int(data['meta']['total']) / int(data['meta']['per_page']))
        for i in range(1, pages):
            task = get_wiki_pages(guid, i + 1, result)
            tasks.append(task)

    await asyncio.gather(*tasks)
    pages_as_list = []
    # through the magic of async all our pages have loaded.
    for page in list(result.values()):
        pages_as_list += page

    write_tasks = []
    for page in pages_as_list:
        write_tasks.append(write_wiki_content(page))

    await asyncio.gather(*write_tasks)

def run_wiki_dump(guid):
    try:
        asyncio.run(main(guid))
    except Exception:
        # There is already an event loop running. Ooops!
        loop = asyncio.get_event_loop()
        loop.create_task(main(guid))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-id',
        '--guid',
        help='The guid of the registration of who\'s wiki you want to dump.',
        required=True
    )
    args = parser.parse_args()

    guid = args.guid
    asyncio.run(main(guid))
