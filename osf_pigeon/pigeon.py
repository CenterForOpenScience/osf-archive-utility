import re
import time
import json
import os
from io import BytesIO
from datetime import datetime
import internetarchive

import tempfile
import math
import asyncio
import requests

from typing import Tuple, Dict
from ratelimit import sleep_and_retry
from ratelimit.exception import RateLimitException
from datacite import DataCiteMDSClient

from osf_pigeon import settings
import zipfile
import bagit


def get_id(metadata, collection=False):
    """
    Naming scheme for osf items
    `{django app name}-{type}-{guid/_id}-{date registered if registration}-{version number}`

    date registered uses the format: %Y-%m-%dT%H-%M-%S.%f to avoid illegal char `:`

    Collections have the prefix, `collection-` for registrations

    :param metadata:
    :return:
    """
    date = datetime.strptime(
        metadata["data"]["attributes"]["date_registered"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).strftime("%Y-%m-%dT%H-%M-%S.%f")

    item_id = f'osf-{metadata["data"]["type"]}-{metadata["data"]["id"]}-{date}-{settings.ID_VERSION}'
    if collection:
        return f"collection-{item_id}"
    return item_id


def get_provider_id(metadata):
    """
    Naming scheme for osf items
    `{django app name}-{type}-{guid/_id}-{date registered if registration}-{version number}`

    date registered uses the format: %Y-%m-%dT%H-%M-%S.%f to avoid illegal char `:`

    Collections have the prefix, `collection-` in the current scheme providers are always
    collections.

    :param metadata:
    :return:
    """
    return (
        f"collection-osf-registration-providers"
        f'-{metadata["data"]["relationships"]["provider"]["data"]["id"]}-{settings.ID_VERSION}'
    )


def get_and_write_file_data_to_temp(url, temp_dir, dir_name):
    response = get_with_retry(url)
    with open(os.path.join(temp_dir, dir_name), "wb") as fp:
        fp.write(response.content)


def get_and_write_json_to_temp(url, temp_dir, filename, parse_json=None):
    pages = asyncio.run(get_paginated_data(url, parse_json))
    with open(os.path.join(temp_dir, filename), "w") as fp:
        fp.write(json.dumps(pages))


def bag_and_tag(
    temp_dir,
    guid,
    datacite_username=settings.DATACITE_USERNAME,
    datacite_password=settings.DATACITE_PASSWORD,
    datacite_prefix=settings.DATACITE_PREFIX,
):

    doi = build_doi(guid)
    xml_metadata = get_datacite_metadata(
        doi, datacite_username, datacite_password, datacite_prefix
    )

    with open(os.path.join(temp_dir, "datacite.xml"), "w") as fp:
        fp.write(xml_metadata)

    bagit.make_bag(temp_dir)
    bag = bagit.Bag(temp_dir)
    assert bag.is_valid()


def create_zip_data(temp_dir):
    zip_data = BytesIO()
    with zipfile.ZipFile(zip_data, "w") as zip_file:
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                file_name = re.sub(f"^{temp_dir}", "", file_path)
                zip_file.write(file_path, arcname=file_name)
    zip_data.seek(0)
    return zip_data


def format_metadata_for_ia_item(json_metadata):
    """
    This is meant to take the response JSON metadata and format it for IA buckets, this is not
    used to generate JSON to be uploaded as raw data into the buckets.
    :param json_metadata:
    :return:
    """

    date_string = json_metadata["data"]["attributes"]["date_created"]
    date_string = date_string.partition(".")[0]
    date_time = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")

    ia_metadata = dict(
        title=json_metadata["data"]["attributes"]["title"],
        description=json_metadata["data"]["attributes"]["description"],
        date_created=date_time.strftime("%Y-%m-%d"),
        contributor="Center for Open Science",
    )

    article_doi = json_metadata["data"]["attributes"]["article_doi"]
    if article_doi:
        ia_metadata["external-identifier"] = f"urn:doi:{article_doi}"

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


def build_doi(guid):
    return settings.DOI_FORMAT.format(prefix=settings.DATACITE_PREFIX, guid=guid)


def get_datacite_metadata(doi, datacite_username, datacite_password, datacite_prefix):
    assert isinstance(datacite_password, str), "Datacite password not passed to pigeon"
    assert isinstance(datacite_username, str), "Datacite username not passed to pigeon"
    assert isinstance(datacite_prefix, str), "Datacite prefix not passed to pigeon"
    client = DataCiteMDSClient(
        url=settings.DATACITE_URL,
        username=datacite_username,
        password=datacite_password,
        prefix=datacite_prefix,
    )
    return ""


@sleep_and_retry
def get_with_retry(
    url, retry_on: Tuple[int] = (), sleep_period: int = None, headers: Dict = None
) -> requests.Response:

    if not headers:
        headers = {}

    if not settings.OSF_USER_THROTTLE_ENABLED:
        assert (
            settings.OSF_BEARER_TOKEN
        ), "must have OSF_BEARER_TOKEN set to disable the api user throttle of the OSF"
        headers["Authorization"] = settings.OSF_BEARER_TOKEN

    resp = requests.get(url, headers=headers)
    if resp.status_code in retry_on:
        raise RateLimitException(
            message="Too many requests, sleeping.",
            period_remaining=sleep_period or int(resp.headers.get("Retry-After") or 0),
        )  # This will be caught by @sleep_and_retry and retried
    resp.raise_for_status()

    return resp


async def get_pages(url, page, result={}, parse_json=None):
    url = f"{url}?page={page}"
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
        contributor_data["ORCiD"] = embed_data["attributes"]["social"].get(
            "orcid", None
        )
        contributor_data["name"] = embed_data["attributes"]["full_name"]
        links = embed_data["relationships"]["institutions"]["links"]
        institution_url = links["related"]["href"]
        institution_response = get_with_retry(institution_url, retry_on=(429,))
        institution_data = institution_response.json()["data"]
        institution_list = [
            institution["attributes"]["name"] for institution in institution_data
        ]
        contributor_data["affiliated_institutions"] = institution_list
        contributor_data_list.append(contributor_data)
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


def get_ia_item(guid, ia_access_key, ia_secret_key):
    session = internetarchive.get_session(
        config={"s3": {"access": ia_access_key, "secret": ia_secret_key}}
    )
    return session.get_item(guid)


def sync_metadata(item_name, metadata, ia_access_key, ia_secret_key):
    ia_item = get_ia_item(item_name, ia_access_key, ia_secret_key)

    if metadata.get("moderation_state") == "withdrawn":  # withdrawn == not searchable
        metadata["noindex"] = True

    modify_metadata_with_retry(ia_item, metadata)


def find_subcollection_for_registration(metadata, ia_access_key, ia_secret_key):
    """
    We are following the typical osf node structure with one quirk, components with no siblings are
     all pointed to the first parent directory with multiple children. All root nodes are in a
     provider collection which is defined outside of this repo.

    :param metadata:
    :param ia_access_key:
    :param ia_secret_key:
    :return:
    """
    if (
        metadata["data"]["relationships"]["parent"]["data"] is None
    ):  # is root, gets own collection
        root_collection_id = get_id(metadata, collection=True)
        create_subcollection(
            root_collection_id,
            ia_access_key,
            ia_secret_key,
            metadata={
                "title": f'Collection for {metadata["data"]["attributes"]["title"]}'
            },
            parent_collection=get_provider_id(metadata),
        )
        return root_collection_id
    else:
        parent_url = metadata["data"]["relationships"]["parent"]["links"]["related"][
            "href"
        ]
        parent_data = asyncio.run(
            get_paginated_data(
                f"{parent_url}?embed=children&embed=parent&version=2.20&embed=children"
            )
        )
        if parent_data["data"]["embeds"]["children"]["meta"]["total"] == 1:
            # This is an only child recurse up to reach grandparent with multiple children or root
            return find_subcollection_for_registration(
                parent_data, ia_access_key, ia_secret_key
            )

        else:
            parent_collection_id = get_id(parent_data, collection=True)
            errors = parent_data["data"]["embeds"]["parent"].get("errors")
            if errors and errors[0]["detail"] == "Not found.":
                create_subcollection(
                    parent_collection_id,
                    ia_access_key,
                    ia_secret_key,
                    metadata={
                        "title": f'Collection for {parent_data["data"]["attributes"]["title"]}'
                    },
                    parent_collection=get_provider_id(metadata),
                )
            else:
                create_subcollection(
                    parent_collection_id,
                    ia_access_key,
                    ia_secret_key,
                    metadata={
                        "title": f'Collection for {parent_data["data"]["attributes"]["title"]}'
                    },
                    parent_collection=get_id(parent_data, collection=True),
                )
            return get_id(parent_data, collection=True)


def create_subcollection(
    collection_id,
    ia_access_key,
    ia_secret_key,
    metadata=None,
    parent_collection=None,
    retries=3,
):
    """
    The expected sub-collection hierarchy is as follows top-level OSF collection -> provider collection -> collection for nodes
    with multiple children -> all only child nodes

    :param metadata: dict should attributes for the provider's sub-collection is being created
    :param parent_collection: str the name of the  sub-collection's parent
    :param ia_access_key: Internet Archive's access key
    :param ia_secret_key: Internet Archive's secret key
    :return:
    """
    if metadata is None:
        metadata = {}

    session = internetarchive.get_session(
        config={"s3": {"access": ia_access_key, "secret": ia_secret_key}}
    )

    collection = internetarchive.Item(session, collection_id)
    parent_collection = parent_collection or settings.OSF_COLLECTION_NAME
    try:
        collection.upload(
            files={"dummy.txt": BytesIO(b"")},
            metadata={
                "mediatype": "collection",
                "collection": parent_collection,
                **metadata,
            },
        )
    except requests.exceptions.HTTPError as e:
        # You don't have permission to join because it hasn't be created yet
        if (
            "Access Denied - You lack sufficient privileges to write to those collections"
            in str(e)
            and retries
        ):
            retries -= 1
            create_subcollection(
                parent_collection,
                ia_access_key,
                ia_secret_key,
                parent_collection=parent_collection,
                retries=retries,
            )
        else:
            raise e


def upload(
    item_name, tmp_dir, metadata, ia_access_key, ia_secret_key, collection_id=None, retries=3
):
    ia_item = get_ia_item(item_name, ia_access_key, ia_secret_key)
    if collection_id is None:
        collection_id = find_subcollection_for_registration(
            metadata, ia_access_key, ia_secret_key
        )

    ia_metadata = format_metadata_for_ia_item(metadata)

    try:
        ia_item.upload(
            {"bag.zip": create_zip_data(tmp_dir)},
            metadata={"collection": collection_id, **ia_metadata},
            access_key=ia_access_key,
            secret_key=ia_secret_key,
        )
    except requests.exceptions.HTTPError as e:
        # You don't have permission to join because a collection because it might not have been created yet.
        if (
            "Access Denied - You lack sufficient privileges to write to those collections"
            in str(e) and retries
        ):
            retries -= 1
            upload(
                item_name,
                create_zip_data(tmp_dir),
                metadata,
                ia_access_key,
                ia_secret_key,
                collection_id=collection_id,
                retries=retries
            )
        else:
            raise e

    return ia_item


def main(
    guid,
    datacite_username=settings.DATACITE_USERNAME,
    datacite_password=settings.DATACITE_PASSWORD,
    datacite_prefix=settings.DATACITE_PREFIX,
    ia_access_key=settings.IA_ACCESS_KEY,
    ia_secret_key=settings.IA_SECRET_KEY,
    osf_api_url=settings.OSF_API_URL,
    osf_files_url=settings.OSF_FILES_URL,
    osf_bearer_token=settings.OSF_BEARER_TOKEN
):

    settings.OSF_BEARER_TOKEN = osf_bearer_token
    assert isinstance(
        ia_access_key, str
    ), "Internet Archive access key was not passed to pigeon"
    assert isinstance(
        ia_secret_key, str
    ), "Internet Archive secret key not passed to pigeon"

    with tempfile.TemporaryDirectory() as temp_dir:
        get_and_write_file_data_to_temp(
            f"{osf_files_url}v1/resources/{guid}/providers/osfstorage/?zip=",
            temp_dir,
            "archived_files.zip",
        )
        get_and_write_json_to_temp(
            f"{osf_api_url}v2/registrations/{guid}/wikis/",
            temp_dir,
            "wikis.json",
        )
        get_and_write_json_to_temp(
            f"{osf_api_url}v2/registrations/{guid}/logs/",
            temp_dir,
            "logs.json",
        )
        get_and_write_json_to_temp(
            f"{osf_api_url}v2/guids/{guid}?embed=parent&embed=children&version=2.20",
            temp_dir,
            "registration.json",
        )
        get_and_write_json_to_temp(
            f"{osf_api_url}v2/registrations/{guid}/contributors/",
            temp_dir,
            "contributors.json",
            parse_json=get_contributors,
        )
        bag_and_tag(
            temp_dir,
            guid,
            datacite_username=datacite_username,
            datacite_password=datacite_password,
            datacite_prefix=datacite_prefix,
        )

        with open(os.path.join(temp_dir, "data", "registration.json"), "r") as f:
            metadata = json.loads(f.read())

        upload(get_id(metadata), temp_dir, metadata, ia_access_key, ia_secret_key)
