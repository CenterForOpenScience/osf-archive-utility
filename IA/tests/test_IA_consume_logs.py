import os
import mock
import json
import unittest
import responses
import settings
from IA.IA_consume_logs import create_logs

HERE = os.path.dirname(os.path.abspath(__file__))


def log_files():
    with open(os.path.join(HERE, 'fixtures/njs82.json')) as json_file:
        return json.loads(json_file.read())


def log_files_two_pages():
    with open(os.path.join(HERE, 'fixtures/8jpzs-1.json')) as json_file:
        page1 = json.loads(json_file.read())
    with open(os.path.join(HERE, 'fixtures/8jpzs-2.json')) as json_file:
        page2 = json.loads(json_file.read())

    return page1, page2


class TestIALogs(unittest.TestCase):

    @responses.activate
    @mock.patch('IA.IA_consume_files.os.mkdir')
    def test_log_dump(self, mock_mkdir):
        responses.add(
            responses.Response(
                responses.GET,
                f'{settings.OSF_API_URL}v2/registrations/njs82/logs/?page[size]=100',
                json=log_files(),
            )
        )

        with mock.patch('builtins.open', mock.mock_open()) as m:
            create_logs('njs82', 'tests', 100, 'asdfasdfasdgfasg', settings.OSF_API_URL)
            m.assert_called_with(os.path.join(HERE, 'njs82/logs/njs82-1.json'), 'w')
            mock_mkdir.assert_called_with(os.path.join(HERE, 'njs82/logs'))

        with open(os.path.join(HERE, 'fixtures/njs82.json')) as json_file:
            source_json = json.loads(json_file.read())
        with open(os.path.join(HERE, 'fixtures/logs/njs82-1.json')) as json_file:
            target_json = json.loads(json_file.read())

        assert source_json['data'] == target_json

    @responses.activate
    @mock.patch('IA.IA_consume_files.os.mkdir')
    def test_log_dump_two_pages(self, mock_mkdir):
        responses.add(
            responses.Response(
                responses.GET,
                f'{settings.OSF_API_URL}v2/registrations/8jpzs/logs/?page[size]=3',
                json=log_files_two_pages()[0],
            )
        )

        responses.add(
            responses.Response(
                responses.GET,
                f'{settings.OSF_API_URL}v2/registrations/'
                f'8jpzs/logs/?format=json&page=2&page%5Bsize%5D=3',
                json=log_files_two_pages()[1],
            )
        )

        with mock.patch('builtins.open', mock.mock_open()) as m:
            create_logs('8jpzs', 'tests', 3, 'asdfasdfasdgfasg', settings.OSF_API_URL)
            m.assert_called_with(os.path.join(HERE, '8jpzs/logs/8jpzs-2.json'), 'w')
            mock_mkdir.assert_called_with(os.path.join(HERE, '8jpzs/logs'))

        with open(os.path.join(HERE, 'fixtures/8jpzs-1.json')) as json_file:
            source_json_1 = json.loads(json_file.read())
        with open(os.path.join(HERE, 'fixtures/8jpzs-2.json')) as json_file:
            source_json_2 = json.loads(json_file.read())
        with open(os.path.join(HERE, 'fixtures/logs/8jpzs-1.json')) as json_file:
            target_json_1 = json.loads(json_file.read())
        with open(os.path.join(HERE, 'fixtures/logs/8jpzs-2.json')) as json_file:
            target_json_2 = json.loads(json_file.read())

        source_json = source_json_1['data'] + source_json_2['data']
        target_json = target_json_1 + target_json_2

        assert source_json == target_json
