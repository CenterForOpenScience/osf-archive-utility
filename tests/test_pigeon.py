import os
from io import BytesIO
import json
import pytest
import mock
from osf_pigeon import settings
from asyncio import run

import responses
import tempfile
from osf_pigeon.pigeon import (
    get_and_write_file_data_to_temp,
    get_and_write_json_to_temp,
    create_zip_data,
    format_metadata_for_ia_item,
    get_contributors,
    sync_metadata,
    upload,
    write_datacite_metadata,
)
import zipfile
from osf_pigeon.settings import ID_VERSION

HERE = os.path.dirname(os.path.abspath(__file__))


class TestGetAndWriteFileDataToTemp:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def zip_name(self):
        return "archived_files.zip"

    @pytest.fixture
    def zip_data(self):
        return b"Brian Dawkins on game day"

    def test_get_and_write_file_data_to_temp(
        self, mock_waterbutler, guid, zip_name, zip_data
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            get_and_write_file_data_to_temp(
                f"{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=",
                temp_dir,
                zip_name,
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == zip_name
            assert open(os.path.join(temp_dir, zip_name), "rb").read() == zip_data


class TestGetAndWriteJSONToTemp:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def file_name(self):
        return "info.json"

    @pytest.fixture
    def json_data(self):
        with open(
            os.path.join(HERE, "fixtures/metadata-resp-with-embeds.json"), "rb"
        ) as fp:
            return fp.read()

    def test_get_and_write_file_data_to_temp(
        self, mock_osf_api, guid, json_data, file_name
    ):
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/guids/{guid}",
            status=200,
            body=json_data,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            run(
                get_and_write_json_to_temp(
                    f"{settings.OSF_API_URL}v2/guids/{guid}", temp_dir, file_name
                )
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == file_name
            info = json.loads(open(os.path.join(temp_dir, file_name)).read())
            assert info == json.loads(json_data)


class TestGetAndWriteJSONToTempMultipage:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def file_name(self):
        return "wikis.json"

    @pytest.fixture
    def page1(self):
        with open(
            os.path.join(HERE, "fixtures/wiki-metadata-response-page-1.json"), "r"
        ) as fp:
            return fp.read()

    @pytest.fixture
    def page2(self):
        with open(
            os.path.join(HERE, "fixtures/wiki-metadata-response-page-2.json"), "r"
        ) as fp:
            return fp.read()

    @pytest.fixture
    def expected_json(self, page1, page2):
        page1 = json.loads(page1)
        page2 = json.loads(page2)

        return page1["data"] + page2["data"]

    def test_get_and_write_file_data_to_temp(
        self, mock_osf_api, guid, page1, page2, file_name, expected_json
    ):
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/{guid}/wikis/",
            status=200,
            body=page1,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/{guid}/wikis/",
            status=200,
            body=page2,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            run(
                get_and_write_json_to_temp(
                    f"{settings.OSF_API_URL}v2/registrations/{guid}/wikis/",
                    temp_dir,
                    file_name,
                )
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == file_name
            info = json.loads(open(os.path.join(temp_dir, file_name)).read())
            assert len(info) == 11
            assert info == expected_json


class TestContributors:
    @pytest.fixture
    def guid(self):
        return "ft3ae"

    @pytest.fixture
    def file_name(self):
        return "contributors.json"

    @pytest.fixture
    def contributors_file(self):
        with open(os.path.join(HERE, "fixtures/ft3ae-contributors.json"), "r") as fp:
            return fp.read()

    @pytest.fixture
    def institutions_file(self):
        with open(os.path.join(HERE, "fixtures/ft3ae-institutions.json"), "r") as fp:
            return fp.read()

    def test_get_and_write_file_data_to_temp(
        self, mock_osf_api, guid, contributors_file, institutions_file, file_name
    ):
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/{guid}/contributors/",
            status=200,
            body=contributors_file,
        )
        mock_osf_api.add(
            responses.GET,
            "http://localhost:8000/v2/users/s3rbx/institutions/",
            status=200,
            body=institutions_file,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            run(
                get_and_write_json_to_temp(
                    f"{settings.OSF_API_URL}v2/registrations/{guid}/contributors/",
                    temp_dir,
                    file_name,
                    parse_json=get_contributors,
                )
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == file_name
            info = json.loads(open(os.path.join(temp_dir, file_name)).read())["data"]
            assert len(info) == 1
            assert (
                info[0]["embeds"]["users"]["data"]["attributes"]["social"]["orcid"]
                == "0000-0001-4934-3444"
            )
            assert info[0]["affiliated_institutions"] == ["Center For Open Science"]


class TestDatacite:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def metadata(self):
        with open(
            os.path.join(HERE, "fixtures/metadata-resp-with-embeds.json"), "rb"
        ) as fp:
            return json.loads(fp.read())

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    def test_get_datacite_metadata(self, guid, mock_datacite, temp_dir, metadata):
        xml = run(write_datacite_metadata(guid, temp_dir, metadata))
        assert xml == "pretend this is XML."


class TestCreateZipData:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def test_file(self, temp_dir):
        with open(os.path.join(temp_dir, "test_file.txt"), "wb") as fp:
            fp.write(b"partytime")

    def test_create_zip_data(self, temp_dir, test_file):
        zip_data = create_zip_data(temp_dir)
        zip_file = zipfile.ZipFile(zip_data)

        assert len(zip_file.infolist()) == 1
        assert zip_file.infolist()[0].filename == "test_file.txt"
        zip_file.extract("test_file.txt", temp_dir)  # just to read

        assert (
            open(os.path.join(temp_dir, "test_file.txt"), "rb").read() == b"partytime"
        )


class TestMetadata:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def zip_data(self):
        return BytesIO(b"Clyde Simmons is underrated")

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def metadata(self):
        with open(
            os.path.join(HERE, "fixtures/metadata-resp-with-embeds.json"), "rb"
        ) as fp:
            return json.loads(fp.read())

    @pytest.fixture
    def registration_children_sparse(self):
        with open(
            os.path.join(HERE, "fixtures/sparse-registration-children.json"), "rb"
        ) as fp:
            return fp.read()

    @pytest.fixture
    def test_node_json(self, temp_dir):
        os.mkdir(os.path.join(temp_dir, "data"))
        with open(
            os.path.join(HERE, "fixtures/metadata-resp-with-embeds.json"), "rb"
        ) as json_fp:
            with open(os.path.join(temp_dir, "data", "test.json"), "wb") as fp:
                fp.write(json_fp.read())
        yield

    @pytest.fixture
    def institutions_json(self):
        with open(os.path.join(HERE, "fixtures/institutions.json"), "rb") as fp:
            return fp.read()

    @pytest.fixture
    def biblio_contribs(self):
        with open(os.path.join(HERE, "fixtures/biblio-contribs.json"), "rb") as fp:
            return fp.read()

    def test_format_metadata_for_ia_item(
        self,
        metadata,
        registration_children_sparse,
        mock_osf_api,
        institutions_json,
        biblio_contribs,
    ):
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/pkdm6/children/"
            f"?fields%5Bregistrations%5D=id",
            body=registration_children_sparse,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/contributors/"
            f"?filter%5Bbibliographic%5D=True"
            f"&fields%5Busers%5D=full_name",
            body=biblio_contribs,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/institutions/",
            body=institutions_json,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/pkdm6/children/?fields%5Bregistrations%5D=id",
            body=registration_children_sparse,
        )
        metadata = run(format_metadata_for_ia_item(metadata))
        assert metadata == {
            "title": "Test Component",
            "description": "Test Description",
            "date_created": "2017-12-20",
            "contributor": "Center for Open Science",
            "category": "",
            "license": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
            "tags": [],
            "contributors": ["John Tordoff"],
            "article_doi": "",
            "registration_doi": "10.70102/osf.io/guid0",
            "children": [
                f"https://archive.org/details/osf-registrations-hbs3p-{ID_VERSION}",
                f"https://archive.org/details/osf-registrations-ec9db-{ID_VERSION}",
            ],
            "registry": "OSF Registries",
            "registration_schema": "Open-Ended Registration",
            "registered_from": "http://localhost:8000/v2/nodes/g752b/",
            "affiliated_institutions": ["The Center For Open Science [Stage]"],
            "parent": f"https://archive.org/details/osf-registrations-dgkjr-{ID_VERSION}",
        }

    def test_modify_metadata_only(self, mock_ia_client, guid):
        metadata = {
            "title": "Test Component",
            "description": "Test Description",
            "date": "2017-12-20",
            "contributor": "Center for Open Science",
        }
        sync_metadata(guid, metadata)
        mock_ia_client.session.get_item.assert_called_with("guid0")
        mock_ia_client.item.modify_metadata.assert_called_with(metadata)

    def test_modify_metadata_not_public(self, mock_ia_client, guid):
        metadata = {
            "title": "Test Component",
            "description": "Test Description",
            "date": "2017-12-20",
            "contributor": "Center for Open Science",
            "is_public": False,
            "moderation_state": "withdrawn",
        }
        sync_metadata(guid, metadata.copy())
        mock_ia_client.session.get_item.assert_called_with("guid0")

        metadata["noindex"] = True
        metadata[
            "description"
        ] = "Note this registration has been withdrawn: \nTest Description"
        mock_ia_client.item.modify_metadata.assert_called_with(metadata)


class TestUpload:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def zip_data(self):
        return BytesIO(b"Clyde Simmons is underrated")

    @pytest.fixture
    def metadata(self):
        with open(
            os.path.join(HERE, "fixtures/metadata-resp-with-embeds.json"), "rb"
        ) as fp:
            return json.loads(fp.read())

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def registration_children_sparse(self):
        with open(
            os.path.join(HERE, "fixtures/sparse-registration-children.json"), "rb"
        ) as fp:
            return fp.read()

    @pytest.fixture
    def institutions_json(self):
        with open(os.path.join(HERE, "fixtures/institutions.json"), "rb") as fp:
            return fp.read()

    @pytest.fixture
    def biblio_contribs(self):
        with open(os.path.join(HERE, "fixtures/biblio-contribs.json"), "rb") as fp:
            return fp.read()

    def test_upload(
        self,
        mock_ia_client,
        mock_osf_api,
        guid,
        zip_data,
        registration_children_sparse,
        biblio_contribs,
        metadata,
        institutions_json,
    ):
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/children/"
            f"?fields%5Bregistrations%5D=id",
            body=registration_children_sparse,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/contributors/"
            f"?filter%5Bbibliographic%5D=True"
            f"&fields%5Busers%5D=full_name",
            body=biblio_contribs,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/institutions/",
            body=institutions_json,
        )
        run(
            upload(
                guid,
                zip_data,
                metadata,
            )
        )

        mock_ia_client.session.get_item.assert_called_with("guid0")
        mock_ia_client.item.upload.assert_called_with(
            mock.ANY,
            metadata={
                "collection": f"collection-osf-registration-providers-osf-{ID_VERSION}",
                "title": "Test Component",
                "description": "Test Description",
                "date_created": "2017-12-20",
                "contributor": "Center for Open Science",
                "category": "",
                "license": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
                "tags": [],
                "contributors": ["John Tordoff"],
                "article_doi": "",
                "registration_doi": "10.70102/osf.io/guid0",
                "children": [
                    f"https://archive.org/details/osf-registrations-hbs3p-{ID_VERSION}",
                    f"https://archive.org/details/osf-registrations-ec9db-{ID_VERSION}",
                ],
                "registry": "OSF Registries",
                "registration_schema": "Open-Ended Registration",
                "registered_from": "http://localhost:8000/v2/nodes/g752b/",
                "affiliated_institutions": ["The Center For Open Science [Stage]"],
                "parent": "https://archive.org/details/osf-registrations-dgkjr-local_v1",
            },
            secret_key=settings.IA_SECRET_KEY,
            access_key=settings.IA_ACCESS_KEY,
        )

    def test_upload_with_different_provider(
        self,
        mock_ia_client,
        mock_osf_api,
        guid,
        zip_data,
        registration_children_sparse,
        biblio_contribs,
        metadata,
        institutions_json,
    ):
        """
        Different providers should get uploaded to different collections
        """
        metadata["data"]["embeds"]["provider"]["data"]["id"] = "burds"

        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/children/"
            f"?fields%5Bregistrations%5D=id",
            body=registration_children_sparse,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/contributors/"
            f"?filter%5Bbibliographic%5D=True"
            f"&fields%5Busers%5D=full_name",
            body=biblio_contribs,
        )
        mock_osf_api.add(
            responses.GET,
            f"{settings.OSF_API_URL}v2/registrations/8gqkv/institutions/",
            body=institutions_json,
        )
        run(
            upload(
                guid,
                zip_data,
                metadata,
            )
        )
        mock_ia_client.session.get_item.assert_called_with("guid0")
        mock_ia_client.item.upload.assert_called_with(
            mock.ANY,
            metadata={
                "collection": f"collection-osf-registration-providers-burds-{ID_VERSION}",
                "title": "Test Component",
                "description": "Test Description",
                "date_created": "2017-12-20",
                "contributor": "Center for Open Science",
                "category": "",
                "license": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
                "tags": [],
                "contributors": ["John Tordoff"],
                "article_doi": "",
                "registration_doi": "10.70102/osf.io/guid0",
                "children": [
                    f"https://archive.org/details/osf-registrations-hbs3p-{ID_VERSION}",
                    f"https://archive.org/details/osf-registrations-ec9db-{ID_VERSION}",
                ],
                "registry": "OSF Registries",
                "registration_schema": "Open-Ended Registration",
                "registered_from": "http://localhost:8000/v2/nodes/g752b/",
                "affiliated_institutions": ["The Center For Open Science [Stage]"],
                "parent": "https://archive.org/details/osf-registrations-dgkjr-local_v1",
            },
            secret_key=settings.IA_SECRET_KEY,
            access_key=settings.IA_ACCESS_KEY,
        )
