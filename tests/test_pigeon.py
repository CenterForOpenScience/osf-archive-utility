import os
from io import BytesIO
import json
import pytest
import mock
from osf_pigeon import settings

import responses
import tempfile
from osf_pigeon.pigeon import (
    get_and_write_file_data_to_temp,
    get_and_write_json_to_temp,
    bag_and_tag,
    create_zip_data,
    format_metadata_for_ia_item,
    modify_metadata_with_retry,
    get_contributors,
    sync_metadata,
    find_subcollection_for_registration,
    upload,
)
import internetarchive
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
            get_and_write_json_to_temp(
                f"{settings.OSF_API_URL}v2/guids/{guid}", temp_dir, file_name
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
            get_and_write_json_to_temp(
                f"{settings.OSF_API_URL}v2/registrations/{guid}/wikis/",
                temp_dir,
                file_name,
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
            get_and_write_json_to_temp(
                f"{settings.OSF_API_URL}v2/registrations/{guid}/contributors/",
                temp_dir,
                file_name,
                parse_json=get_contributors,
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == file_name
            info = json.loads(open(os.path.join(temp_dir, file_name)).read())["data"]
            assert len(info) == 1
            assert info[0]["ORCiD"] == "0000-0001-4934-3444"
            assert info[0]["affiliated_institutions"] == ["Center For Open Science"]


class TestBagAndTag:
    @pytest.fixture
    def guid(self):
        return "guid0"

    def test_bag_and_tag(self, guid, mock_datacite):
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch("bagit.Bag") as mock_bag:
                bag_and_tag(
                    temp_dir, guid, "test datcite password", "test datcite username"
                )
                mock_bag.assert_called_with(temp_dir)


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
        with open(os.path.join(HERE, "fixtures/root-registration.json"), "rb") as fp:
            return json.loads(fp.read())

    @pytest.fixture
    def test_node_json(self, temp_dir):
        os.mkdir(os.path.join(temp_dir, "data"))
        with open(
            os.path.join(HERE, "fixtures/metadata-resp-with-embeds.json"), "rb"
        ) as json_fp:
            with open(os.path.join(temp_dir, "data", "test.json"), "wb") as fp:
                fp.write(json_fp.read())
        yield

    def test_format_metadata_for_ia_item(self, metadata):
        metadata = format_metadata_for_ia_item(metadata)
        assert metadata == {
            "title": "Root Registration with no children",
            "description": "This is a fake registration to test how to structure our project.",
            "date_created": "2021-02-08",
            "contributor": "Center for Open Science",
        }

    def test_modify_metadata(self, temp_dir, test_node_json):
        metadata = {
            "title": "Test Component",
            "description": "Test Description",
            "date": "2017-12-20",
            "contributor": "Center for Open Science",
        }
        mock_ia_item = mock.Mock()
        modify_metadata_with_retry(mock_ia_item, metadata)

        assert len(mock_ia_item.mock_calls) == 1
        assert mock_ia_item.mock_calls[0][1][0] == metadata

    def test_modify_metadata_only(self, mock_ia_client, guid):
        metadata = {
            "title": "Test Component",
            "description": "Test Description",
            "date": "2017-12-20",
            "contributor": "Center for Open Science",
        }
        sync_metadata(guid, metadata, "notrealaccesskey", "notrealsecretkey")
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
        sync_metadata(guid, metadata.copy(), "notrealaccesskey", "notrealsecretkey")
        mock_ia_client.session.get_item.assert_called_with("guid0")

        metadata["noindex"] = True
        mock_ia_client.item.modify_metadata.assert_called_with(metadata)

    def test_modify_metadata_with_retry(self, temp_dir, test_node_json):
        metadata = {
            "title": "Test Component",
            "description": "Test Description",
            "date": "2017-12-20",
            "contributor": "Center for Open Science",
        }
        mock_ia_item = mock.Mock()
        mock_ia_item.modify_metadata = mock.Mock(
            side_effect=internetarchive.exceptions.ItemLocateError()
        )

        with pytest.raises(internetarchive.exceptions.ItemLocateError):
            modify_metadata_with_retry(
                mock_ia_item, metadata, sleep_time=1  # 1 second for fast tests
            )

        assert len(mock_ia_item.mock_calls) == 3

        for call in mock_ia_item.mock_calls:
            assert call[1][0] == metadata


class TestSubcollections:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def root_metadata(self):
        with open(os.path.join(HERE, "fixtures/root-registration.json"), "rb") as fp:
            return json.loads(fp.read())

    @pytest.fixture
    def child_metadata(self):
        with open(os.path.join(HERE, "fixtures/child-registration.json"), "rb") as fp:
            return fp.read()

    @pytest.fixture
    def parent_with_children(self):
        with open(
            os.path.join(HERE, "fixtures/parent-with-children-embed.json"), "rb"
        ) as fp:
            return fp.read()

    @pytest.fixture
    def parent_with_one_child(self):
        with open(
            os.path.join(HERE, "fixtures/parent-with-child-embed.json"), "rb"
        ) as fp:
            return fp.read()

    def test_create_subcollection(self):
        pass

    def test_find_subcollection_root(self, mock_ia_client, root_metadata):
        find_subcollection_for_registration(root_metadata, "notreal", "fake")

        mock_ia_client.assert_called_with(
            config={"s3": {"access": "notreal", "secret": "fake"}}
        )

        mock_ia_client.item.upload.assert_called_with(
            files={"dummy.txt": mock.ANY},
            metadata={
                "mediatype": "collection",
                "collection": f"collection-osf-registration-providers-osf-{ID_VERSION}",
                "title": "Collection for Root Registration with no children",
            },
        )

    def test_find_subcollection_has_siblings(
        self, mock_ia_client, mock_osf_api, child_metadata, parent_with_children
    ):
        mock_osf_api.add(
            responses.GET,
            "https://api.osf.io/v2/registrations/mdn5w/?"
            "embed=children&embed=parent&version=2.20&embed=children",
            body=parent_with_children,
            status=200,
        )

        find_subcollection_for_registration(
            json.loads(child_metadata), "notreal", "fake"
        )
        mock_ia_client.assert_called_with(
            config={"s3": {"access": "notreal", "secret": "fake"}}
        )
        mock_ia_client.item.upload.assert_called_with(
            files={"dummy.txt": mock.ANY},
            metadata={
                "mediatype": "collection",
                "collection": f"collection-osf-registration-providers-osf-{ID_VERSION}",
                "title": "Collection for Parent with 3 Children",
            },
        )

    def test_find_subcollection_only_child(
        self,
        mock_ia_client,
        mock_osf_api,
        child_metadata,
        parent_with_one_child,
        parent_with_children,
    ):
        mock_osf_api.add(
            responses.GET,
            "https://api.osf.io/v2/registrations/mdn5w/"
            "?embed=children&embed=parent&version=2.20&embed=children",
            body=parent_with_one_child,
            status=200,
        )
        mock_osf_api.add(
            responses.GET,
            "https://api.osf.io/v2/registrations/nope1/"
            "?embed=children&embed=parent&version=2.20&embed=children",
            body=parent_with_children,
            status=200,
        )

        find_subcollection_for_registration(
            json.loads(child_metadata), "notreal", "fake"
        )
        mock_ia_client.assert_called_with(
            config={"s3": {"access": "notreal", "secret": "fake"}}
        )
        mock_ia_client.item.upload.assert_called_with(
            files={"dummy.txt": mock.ANY},
            metadata={
                "mediatype": "collection",
                "collection": f"collection-osf-registration-providers-osf-{ID_VERSION}",
                "title": "Collection for Parent with 3 Children",
            },
        )


class TestUpload:
    @pytest.fixture
    def guid(self):
        return "guid0"

    @pytest.fixture
    def zip_data(self):
        return BytesIO(b"Clyde Simmons is underrated")

    @pytest.fixture
    def temp_dir(self, zip_data):
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(temp_dir, "wb") as fp:
                fp.write(zip_data)
            yield temp_dir

    @pytest.fixture
    def metadata(self):
        with open(os.path.join(HERE, "fixtures/root-registration.json"), "rb") as fp:
            return json.loads(fp.read())

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    def test_upload(self, mock_ia_client, mock_osf_api, guid, temp_dir, metadata):
        upload(
            guid,
            temp_dir,
            metadata,
            ia_access_key="Buddy Ryan",
            ia_secret_key="Fletcher Cox",
        )

        mock_ia_client.session.get_item.assert_called_with("guid0")
        mock_ia_client.item.upload.assert_called_with(
            {"bag.zip": mock.ANY},
            metadata={
                "collection": f"collection-osf-registrations-pkdm6-2021-02-08T23-09-41.562456-{ID_VERSION}",
                "title": "Root Registration with no children",
                "description": "This is a fake registration to test how to structure our project.",
                "date_created": "2021-02-08",
                "contributor": "Center for Open Science",
            },
            access_key="Buddy Ryan",
            secret_key="Fletcher Cox",
        )

    def test_upload_with_different_provider(
        self, mock_ia_client, mock_osf_api, guid, temp_dir, metadata
    ):
        """
        Different providers should get uploaded to different collections
        """
        metadata["data"]["relationships"]["provider"]["data"]["id"] = "burds"

        upload(
            guid,
            temp_dir,
            metadata,
            ia_access_key="Buddy Ryan",
            ia_secret_key="Fletcher Cox",
        )

        mock_ia_client.session.get_item.assert_called_with("guid0")
        mock_ia_client.item.upload.assert_called_with(
            {"bag.zip": mock.ANY},
            metadata={
                "collection": f"collection-osf-registrations-pkdm6-2021-02-08T23-09-41.562456-{ID_VERSION}",
                "title": "Root Registration with no children",
                "description": "This is a fake registration to test how to structure our project.",
                "date_created": "2021-02-08",
                "contributor": "Center for Open Science",
            },
            access_key="Buddy Ryan",
            secret_key="Fletcher Cox",
        )
