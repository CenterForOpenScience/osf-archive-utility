import os
import mock
import unittest
import responses
import settings
from IA.IA_consume_files import consume_files

HERE = os.path.dirname(os.path.abspath(__file__))


class TestIAFiles(unittest.TestCase):

    @responses.activate
    @mock.patch('IA.IA_consume_files.os.mkdir')
    @mock.patch('IA.IA_consume_files.os.remove')
    @mock.patch('IA.IA_consume_files.ZipFile')
    def test_file_dump(self, mock_zipfile, mock_rm, mock_mkdir):
        with open('IA/tests/fixtures/sgg32.zip', 'rb') as zipfile:
            responses.add(
                responses.Response(
                    responses.GET,
                    f'{settings.OSF_FILES_URL}v1/resources/sgg32/providers/osfstorage/?zip=',
                    body=zipfile.read(),
                    status=200,
                    stream=True
                )
            )

        with mock.patch('builtins.open', mock.mock_open()) as m:
            consume_files('sgg32', 'asdfasdfasdgfasg', 'tests/test_folder')
            mock_mkdir.assert_called_with(os.path.join(HERE, 'test_folder/sgg32', 'files'))
            m.assert_called_with(os.path.join(HERE, 'test_folder/sgg32/files/sgg32.zip'), 'wb')
            zip_path = os.path.join(HERE, 'test_folder/sgg32/files/sgg32.zip')
            mock_zipfile.assert_called_with(zip_path, 'r')
            mock_rm.assert_called_with(os.path.join(HERE, 'test_folder/sgg32/files/sgg32.zip'))

    @responses.activate
    @mock.patch('IA.IA_consume_files.os.mkdir')
    @mock.patch('IA.IA_consume_files.os.remove')
    @mock.patch('IA.IA_consume_files.ZipFile')
    def test_file_dump_multiple_levels(self, mock_zipfile, mock_rm, mock_mkdir):
        with open('IA/tests/fixtures/jj81a.zip', 'rb') as zipfile:
            responses.add(
                responses.Response(
                    responses.GET,
                    f'{settings.OSF_FILES_URL}v1/resources/jj81a/providers/osfstorage/?zip=',
                    body=zipfile.read(),
                    status=200,
                    stream=True
                )
            )

        with mock.patch('builtins.open', mock.mock_open()) as m:
            consume_files('jj81a', None, 'tests/test_folder')
            mock_mkdir.assert_called_with(os.path.join(HERE, 'test_folder/jj81a/files'))
<<<<<<< HEAD
<<<<<<< HEAD
            zip_path = os.path.join(HERE, 'test_folder/jj81a/files/jj81a.zip')
            m.assert_called_with(zip_path, 'wb')
            mock_zipfile.assert_called_with(zip_path, 'r')
=======
            m.assert_called_with(os.path.join(HERE, 'test_folder/jj81a/files/jj81a.zip'), 'wb')
=======
            zip_path = os.path.join(HERE, 'test_folder/jj81a/files/jj81a.zip')
            m.assert_called_with(zip_path, 'wb')
>>>>>>> Fixing some flake errors
            mock_zipfile.assert_called_with(os.path.join(HERE, 'test_folder/jj81a/files/jj81a.zip'), 'r')
>>>>>>> Fixing tests more
            mock_rm.assert_called_with(os.path.join(HERE, 'test_folder/jj81a/files/jj81a.zip'))
