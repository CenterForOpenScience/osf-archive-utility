import os
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
    get_metadata,
)
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))


class TestGetAndWriteFileDataToTemp:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    @pytest.fixture
    def zip_name(self):
        return 'archived_files.zip'

    @pytest.fixture
    def zip_data(self):
        return b'Brian Dawkins on game day'

    def test_get_and_write_file_data_to_temp(self, mock_waterbutler, guid, zip_name, zip_data):
        with tempfile.TemporaryDirectory() as temp_dir:
            get_and_write_file_data_to_temp(
                f'{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=',
                temp_dir,
                zip_name
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == zip_name
            assert open(os.path.join(temp_dir, zip_name), 'rb').read() == zip_data


class TestGetAndWriteJSONToTemp:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    @pytest.fixture
    def file_name(self):
        return 'info.json'

    @pytest.fixture
    def json_data(self):
        with open(os.path.join(HERE, 'fixtures/metadata-resp-with-embeds.json'), 'rb') as fp:
            return fp.read()

    def test_get_and_write_file_data_to_temp(self, mock_osf_api, guid, json_data, file_name):
        mock_osf_api.add(
            responses.GET,
            f'{settings.OSF_API_URL}v2/guids/{guid}',
            status=200,
            body=json_data
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            get_and_write_json_to_temp(
                f'{settings.OSF_API_URL}v2/guids/{guid}',
                temp_dir,
                file_name
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == file_name
            info = json.loads(open(os.path.join(temp_dir, file_name)).read())
            assert info == json.loads(json_data)


class TestGetAndWriteJSONToTempMultipage:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    @pytest.fixture
    def file_name(self):
        return 'wikis.json'

    @pytest.fixture
    def page1(self):
        with open(os.path.join(HERE, 'fixtures/wiki-metadata-response-page-1.json'), 'r') as fp:
            return fp.read()

    @pytest.fixture
    def page2(self):
        with open(os.path.join(HERE, 'fixtures/wiki-metadata-response-page-2.json'), 'r') as fp:
            return fp.read()

    @pytest.fixture
    def expected_json(self, page1, page2):
        page1 = json.loads(page1)
        page2 = json.loads(page2)

        return page1['data'] + page2['data']

    def test_get_and_write_file_data_to_temp(
            self,
            mock_osf_api,
            guid,
            page1,
            page2,
            file_name,
            expected_json):
        mock_osf_api.add(
            responses.GET,
            f'{settings.OSF_API_URL}v2/registrations/{guid}/wikis/',
            status=200,
            body=page1
        )
        mock_osf_api.add(
            responses.GET,
            f'{settings.OSF_API_URL}v2/registrations/{guid}/wikis/',
            status=200,
            body=page2
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            get_and_write_json_to_temp(
                f'{settings.OSF_API_URL}v2/registrations/{guid}/wikis/',
                temp_dir,
                file_name
            )
            assert len(os.listdir(temp_dir)) == 1
            assert os.listdir(temp_dir)[0] == file_name
            info = json.loads(open(os.path.join(temp_dir, file_name)).read())
            assert len(info) == 11
            assert info == expected_json


class TestBagAndTag:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    def test_bag_and_tag(self, guid, mock_datacite):
        with tempfile.TemporaryDirectory() as temp_dir:
            with mock.patch('bagit.Bag') as mock_bag:
                bag_and_tag(temp_dir, guid, datacite_password='test', datacite_username='test')
                mock_bag.assert_called_with(temp_dir)


class TestCreateZipData:

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def test_file(self, temp_dir):
        with open(os.path.join(temp_dir, 'test_file.txt'), 'wb') as fp:
            fp.write(b'partytime')

    def test_create_zip_data(self, temp_dir, test_file):
        zip_data = create_zip_data(temp_dir)

        zip_file = zipfile.ZipFile(zip_data)
        assert len(zip_file.infolist()) == 1
        assert zip_file.infolist()[0].filename == 'test_file.txt'
        zip_file.extract('test_file.txt', temp_dir)  # just to read

        assert open(os.path.join(temp_dir, 'test_file.txt'), 'rb').read() == b'partytime'


class TestMetadata:

    @pytest.fixture
    def guid(self):
        return 'guid0'

    @pytest.fixture
    def zip_data(self):
        return b'Clyde Simmons is underrated'

    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def test_node_json(self, temp_dir):
        os.mkdir(os.path.join(temp_dir, 'data'))
        with open(os.path.join(HERE, 'fixtures/metadata-resp-with-embeds.json'), 'rb') as json_fp:
            with open(os.path.join(temp_dir, 'data', 'registration.json'), 'wb') as fp:
                fp.write(json_fp.read())
        yield

    def test_get_metadata(self, temp_dir, test_node_json):
        metadata = get_metadata(temp_dir, 'registration.json')
        assert metadata == {
            'title': 'Test Component',
            'description': 'Test Description',
            'date': '2017-12-20',
            'contributor': 'Center for Open Science',
        }
