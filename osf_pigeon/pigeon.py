import re
import time
import json
import os
from io import BytesIO
from datetime import datetime
import internetarchive
from asyncio import events
from concurrent.futures import ThreadPoolExecutor

import tempfile
import math
import asyncio
import requests

from typing import Tuple, Dict
from datacite import DataCiteMDSClient
from datacite.errors import DataCiteNotFoundError

from osf_pigeon import settings
import zipfile
import bagit
from ratelimit import sleep_and_retry
from ratelimit.exception import RateLimitException


def get_id(guid):
    """
    Naming scheme for osf items
    `{django app name}-{type}-{guid/_id}-{version number}`

    :param guid:
    :return:
    """
    return f"osf-registrations-{guid}-{settings.ID_VERSION}"


def get_provider_id(metadata):
    """
    Naming scheme for osf items
    `{django app name}-{type}-{guid/_id}-{version number}`

    Collections have the prefix, `collection-` in the current scheme providers are always
    collections.

    :param guid:
    :return:
    """
    return f'collection-osf-registration-providers-' \
           f'{metadata["data"]["embeds"]["provider"]["data"]["id"]}-{settings.ID_VERSION}'


async def get_and_write_file_data_to_temp(from_url, to_dir, name):
    with get_with_retry(from_url) as response:
        with open(os.path.join(to_dir, name), "wb") as fp:
            for chunk in response.iter_content():
                fp.write(chunk)


async def get_and_write_json_to_temp(from_url, to_dir, name, parse_json=None):
    pages = await get_paginated_data(from_url, parse_json)
    with open(os.path.join(to_dir, name), "w") as fp:
        json.dump(pages, fp)

    return pages


def create_zip_data(temp_dir):
    zip_data = BytesIO()
    zip_data.name = "bag.zip"
    with zipfile.ZipFile(zip_data, "w") as fp:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_name = re.sub(f"^{temp_dir}", "", file_path)
                fp.write(file_path, arcname=file_name)
    zip_data.seek(0)
    return zip_data


async def format_metadata_for_ia_item(json_metadata):
    """
    This is meant to take the response JSON metadata and format it for IA buckets, this is not
    used to generate JSON to be uploaded as raw data into the buckets.
    :param json_metadata:
    :return:
    """

    date_string = json_metadata["data"]["attributes"]["date_created"]
    date_string = date_string.partition(".")[0]
    date_time = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    biblo_contrbs = await get_paginated_data(
        f'{settings.OSF_API_URL}v2/registrations/{json_metadata["data"]["id"]}/contributors/?'
        f"filter[bibliographic]=True&fields[users]=full_name"
    )
    biblo_contrbs = [
        contrib["embeds"]["users"]["data"]["attributes"]["full_name"]
        for contrib in biblo_contrbs["data"]
    ]

    institutions = (
        await get_paginated_data(
            f'{settings.OSF_API_URL}v2/registrations/{json_metadata["data"]["id"]}/institutions/'
        )
    )["data"]

    embeds = json_metadata["data"]["embeds"]

    #  10 is default page size
    if (
        json_metadata["data"]["relationships"]["children"]["links"]["related"]["meta"][
            "count"
        ]
        > 10
    ):
        children = await get_paginated_data(
            f'{settings.OSF_API_URL}v2/registrations/{json_metadata["data"]["id"]}/children/'
            f"?fields[registrations]=id"
        )
    else:
        children = embeds["children"]["data"]

    doi = next(
        (
            identifier["attributes"]["value"]
            for identifier in embeds["identifiers"]["data"]
            if identifier["attributes"]["category"] == "doi"
        ),
        None,
    )
    article_doi = json_metadata["data"]["attributes"]["article_doi"]
    ia_metadata = {
        "title": json_metadata["data"]["attributes"]["title"],
        "description": json_metadata["data"]["attributes"]["description"],
        "date_created": date_time.strftime("%Y-%m-%d"),
        "contributor": "Center for Open Science",
        "category": json_metadata["data"]["attributes"]["category"],
        "license": embeds["license"]["data"]["attributes"]["url"],
        "tags": json_metadata["data"]["attributes"]["tags"],
        "contributors": biblo_contrbs,
        "article_doi": f"urn:doi:{article_doi}" if article_doi else "",
        "registration_doi": doi,
        "children": [
            f'https://archive.org/details/{get_id(child["id"])}' for child in children
        ],
        "registry": embeds["provider"]["data"]["attributes"]["name"],
        "registration_schema": embeds["registration_schema"]["data"]["attributes"][
            "name"
        ],
        "registered_from": json_metadata["data"]["relationships"]["registered_from"][
            "links"
        ]["related"]["href"],
        "affiliated_institutions": [
            institution["attributes"]["name"] for institution in institutions
        ],
    }

    if json_metadata["data"]["relationships"]["parent"]["data"]:
        parent_id = get_id(
            json_metadata["data"]["relationships"]["parent"]["data"]["id"]
        )
        ia_metadata["parent"] = f"https://archive.org/details/{parent_id}"

    return ia_metadata


def modify_metadata_with_retry(ia_item, metadata, retries=2, sleep_time=60):
    try:
        ia_item.modify_metadata(metadata.copy())
    except internetarchive.exceptions.ItemLocateError as e:
        if "Item cannot be located because it is dark" in str(e) and retries > 0:
            time.sleep(sleep_time)
            retries -= 1
            modify_metadata_with_retry(ia_item, metadata, retries, sleep_time)
        else:
            raise e


async def write_datacite_metadata(guid, temp_dir, metadata):
    doi = [
        identifier["attributes"]["value"]
        for identifier in metadata["data"]["embeds"]["identifiers"]["data"]
        if identifier["attributes"]["category"] == "doi"
    ]
    if not doi:
        raise DataCiteNotFoundError(
            f"Datacite DOI not found for registration {guid} on OSF server."
        )
    else:
        doi = doi[0]
    client = DataCiteMDSClient(
        url=settings.DATACITE_URL,
        username=settings.DATACITE_USERNAME,
        password=settings.DATACITE_PASSWORD,
        prefix=settings.DATACITE_PREFIX,
    )
    try:
        xml_metadata = client.metadata_get(doi)
    except DataCiteNotFoundError:
        raise DataCiteNotFoundError(
            f"Datacite DOI {doi} not found for registration {guid} on Datacite server."
        )

    with open(os.path.join(temp_dir, "datacite.xml"), "w") as fp:
        fp.write(xml_metadata)

    return xml_metadata


@sleep_and_retry
def get_with_retry(
    url, retry_on: Tuple[int] = (), sleep_period: int = None, headers: Dict = None
) -> requests.Response:

    if not headers:
        headers = {}

    if settings.OSF_BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {settings.OSF_BEARER_TOKEN}"

    resp = requests.get(url, headers=headers, stream=True)
    if resp.status_code in retry_on:
        raise RateLimitException(
            message="Too many requests, sleeping.",
            period_remaining=sleep_period or int(resp.headers.get("Retry-After") or 0),
        )  # This will be caught by @sleep_and_retry and retried
    resp.raise_for_status()

    return resp


async def get_pages(url, page, result={}, parse_json=None):
    url = f"{url}?page={page}&page={page}"
    resp = get_with_retry(url, retry_on=(429,))

    result[page] = resp.json()["data"]

    if parse_json:
        result[page] = parse_json(resp.json())["data"]

    return result


def get_contributors(response):
    contributor_data_list = []
    for contributor in response["data"]:
        contributor_data = {}
        embed_data = contributor["embeds"]["users"]["data"]
        institution_url = embed_data["relationships"]["institutions"]["links"][
            "related"
        ]["href"]
        institution_response = get_with_retry(institution_url, retry_on=(429,))
        institution_data = institution_response.json()["data"]
        institution_list = [
            institution["attributes"]["name"] for institution in institution_data
        ]
        contributor_data["affiliated_institutions"] = institution_list
        contributor.update(contributor_data)
        contributor_data_list.append(contributor)
    response["data"] = contributor_data_list
    return response


async def get_paginated_data(url, parse_json=None):
    data = get_with_retry(url, retry_on=(429,)).json()

    tasks = []
    is_paginated = data.get("links", {}).get("next")

    if parse_json:
        data = parse_json(data)

    if is_paginated:
        result = {1: data["data"]}
        total = data["links"].get("meta", {}).get("total") or data["meta"].get("total")
        per_page = data["links"].get("meta", {}).get("per_page") or data["meta"].get(
            "per_page"
        )

        pages = math.ceil(int(total) / int(per_page))
        for i in range(1, pages):
            task = get_pages(url, i + 1, result)
            tasks.append(task)

        await asyncio.gather(*tasks)
        pages_as_list = []
        # through the magic of async all our pages have loaded.
        for page in list(result.values()):
            pages_as_list += page
        return pages_as_list
    else:
        return data


def get_ia_item(guid):
    session = internetarchive.get_session(
        config={
            "s3": {"access": settings.IA_ACCESS_KEY, "secret": settings.IA_SECRET_KEY},
        },
    )
    return session.get_item(guid)


def sync_metadata(item_name, metadata):
    ia_item = get_ia_item(item_name)
    if metadata.get("moderation_state") == "withdrawn":  # withdrawn == not searchable
        metadata["noindex"] = True
    modify_metadata_with_retry(ia_item, metadata)

    return metadata, ia_item.urls.details


def create_subcollection(collection_id, metadata=None, parent_collection=None):
    """
    The expected sub-collection hierarchy is as follows top-level OSF collection -> provider
    collection -> collection for nodes with multiple children -> all only child nodes

    :param metadata: dict should attributes for the provider's sub-collection is being created
    :param parent_collection: str the name of the  sub-collection's parent
    :return:
    """
    if metadata is None:
        metadata = {}

    session = internetarchive.get_session(
        config={
            "s3": {"access": settings.IA_ACCESS_KEY, "secret": settings.IA_SECRET_KEY},
        },
    )

    collection = internetarchive.Item(session, collection_id)
    collection.upload(
        files={"dummy.txt": BytesIO(b"dummy")},
        metadata={
            "mediatype": "collection",
            "collection": parent_collection,
            **metadata,
        },
    )


async def upload(item_name, data, metadata):
    ia_item = get_ia_item(item_name)
    ia_metadata = await format_metadata_for_ia_item(metadata)
    ia_item.upload(
        data,
        metadata={"collection": get_provider_id(metadata), **ia_metadata},
        access_key=settings.IA_ACCESS_KEY,
        secret_key=settings.IA_SECRET_KEY,
    )
    return ia_item


async def get_registration_metadata(guid, temp_dir, filename):
    metadata = await get_paginated_data(
        f"{settings.OSF_API_URL}v2/registrations/{guid}/"
        f"?embed=parent"
        f"&embed=children"
        f"&embed=provider"
        f"&embed=identifiers"
        f"&embed=license"
        f"&embed=registration_schema"
        f"&related_counts=true"
        f"&version=2.20"
    )
    if metadata["data"]["attributes"]["withdrawn"]:
        raise PermissionError(f"Registration {guid} is withdrawn")

    with open(os.path.join(temp_dir, filename), "w") as fp:
        json.dump(metadata, fp)

    return metadata


async def get_raw_data(guid, temp_dir):
    try:
        await get_and_write_file_data_to_temp(
            from_url=f"{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=",
            to_dir=temp_dir,
            name="archived_files.zip",
        )
    except requests.exceptions.ChunkedEncodingError:
        raise requests.exceptions.ChunkedEncodingError(
            f"OSF file system is sending incomplete streams for {guid}"
        )


async def main(guid):
    with tempfile.TemporaryDirectory(prefix=get_id(guid)) as temp_dir:
        # await first to check if withdrawn
        metadata = await get_registration_metadata(guid, temp_dir, "registration.json")

        tasks = [
            write_datacite_metadata(guid, temp_dir, metadata),
            get_and_write_json_to_temp(
                from_url=f"{settings.OSF_API_URL}v2/registrations/{guid}/wikis/"
                f"?page[size]=100",
                to_dir=temp_dir,
                name="wikis.json",
            ),
            get_and_write_json_to_temp(
                from_url=f"{settings.OSF_API_URL}v2/registrations/{guid}/logs/"
                f"?page[size]=100",
                to_dir=temp_dir,
                name="logs.json",
            ),
            get_and_write_json_to_temp(
                from_url=f"{settings.OSF_API_URL}v2/registrations/{guid}/contributors/"
                f"?page[size]=100",
                to_dir=temp_dir,
                name="contributors.json",
                parse_json=get_contributors,
            ),
        ]
        # only download achived data if there are files
        file_count = metadata["data"]["relationships"]["files"]["links"]["related"][
            "meta"
        ]["count"]
        if file_count:
            tasks.append(get_raw_data(guid, temp_dir))

        with ThreadPoolExecutor(max_workers=5) as pool:
            running_tasks = [pool.submit(run, task) for task in tasks]
            for task in running_tasks:
                task.result()

        bagit.make_bag(temp_dir)
        bag = bagit.Bag(temp_dir)
        assert bag.is_valid()

        zip_data = create_zip_data(temp_dir)
        ia_item = await upload(get_id(guid), zip_data, metadata)

        return guid, ia_item.urls.details


def run(main):
    loop = events.new_event_loop()
    try:
        events.set_event_loop(loop)
        return loop.run_until_complete(main)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            events.set_event_loop(None)
            loop.close()
