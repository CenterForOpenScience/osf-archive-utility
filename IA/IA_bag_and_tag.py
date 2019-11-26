import re
import os
import bagit
import argparse
from datacite import DataCiteMDSClient
import settings
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))


def build_doi(guid):
    return settings.DOI_FORMAT.format(prefix=settings.DATACITE_PREFIX, guid=guid)


def get_datacite_metadata(doi):
    client = DataCiteMDSClient(
        url=settings.DATACITE_URL,
        username=settings.DATACITE_USERNAME,
        password=settings.DATACITE_PASSWORD,
        prefix=settings.DATACITE_PREFIX,
    )
    return client.metadata_get(doi)


def bag_and_tag(xml_metadata, destination):
    with open(os.path.join(HERE, destination, 'datacite.xml'), 'w') as fp:
        fp.write(xml_metadata)

    path = os.path.join(HERE, destination)
    bagit.make_bag(path)

    bag = bagit.Bag(path)
    assert bag.is_valid()


def zip_bag(destination):
    path = os.path.join(HERE, destination)
    with zipfile.ZipFile('bag.zip', 'w') as zip_file:
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                file_name = re.sub(f"^{path}", "", file_path)
                zip_file.write(file_path, arcname=file_name)


def main(guid, destination, zip=False):
    doi = build_doi(guid)
    xml_metadata = get_datacite_metadata(doi)
    bag_and_tag(xml_metadata, destination)

    if zip:
        zip_bag(destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g',
        '--guid',
        help='The guid of the registration that you want to get datacite metadata for.',
        required=True,
    )
    parser.add_argument(
        '-d',
        '--destination',
        help='The parent directory a the file is copied into.',
        required=True,
    )
    parser.add_argument(
        '-z',
        '--zip',
        help='A boolean representing if the bag is zipped.',
    )
    args = parser.parse_args()
    guid = args.guid
    destination = args.destination
    zip = args.zip
    main(guid, destination, zip)
