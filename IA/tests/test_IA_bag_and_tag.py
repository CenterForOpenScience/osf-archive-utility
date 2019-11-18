import os
import mock
import json
import unittest
from nose.tools import assert_in
from IA.IA_bag_and_tag import bag_and_tag

HERE = os.path.dirname(os.path.abspath(__file__))


def node_metadata():
    with open(os.path.join(HERE, 'fixtures/metadata-resp-with-embeds.json'), 'r') as fp:
        return json.loads(fp.read())


def datacite_xml():
    with open(os.path.join(HERE, 'fixtures/datacite-metadata.xml'), 'r') as fp:
        return fp.read()


class TestBagAndTag(unittest.TestCase):

    @mock.patch('IA.IA_bag_and_tag.bagit.make_bag')
    def test_bag_and_tag(self, mock_bagit):
        with mock.patch('builtins.open', mock.mock_open()) as m:
            bag_and_tag(datacite_xml(), 'tests/test_directory')
            m.assert_called_with(os.path.join(HERE, 'test_directory/datacite.xml'), 'w')
            assert_in('IA/tests/test_directory', mock_bagit.call_args_list[0][0][0])
