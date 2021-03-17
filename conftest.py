import mock
import pytest
import responses
from osf_pigeon import settings


@pytest.fixture
def mock_waterbutler(guid, zip_data):
    with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
        rsps.add(
            responses.GET,
            f"{settings.OSF_FILES_URL}v1/resources/{guid}/providers/osfstorage/?zip=",
            status=200,
            body=zip_data,
        )
        yield rsps


@pytest.fixture
def mock_osf_api():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        yield rsps


@pytest.fixture
def mock_datacite(guid):

    with mock.patch.object(settings, "DOI_FORMAT", "{prefix}/osf.io/{guid}"):
        doi = settings.DOI_FORMAT.format(prefix=settings.DATACITE_PREFIX, guid=guid)

        with responses.RequestsMock(assert_all_requests_are_fired=True) as rsps:
            rsps.add(
                responses.GET,
                f"{settings.DATACITE_URL}metadata/{doi}",
                status=200,
                body=b"pretend this is XML.",
            )
            yield rsps


@pytest.fixture
def mock_ia_client():
    with mock.patch("osf_pigeon.pigeon.internetarchive.get_session") as mock_ia:
        mock_session = mock.Mock()
        mock_ia_item = mock.Mock()
        mock_ia.return_value = mock_session
        mock_session.get_item.return_value = mock_ia_item

        # ⬇️ we only pass one mock into the test
        mock_ia.session = mock_session
        mock_ia.item = mock_ia_item
        with mock.patch(
            "osf_pigeon.pigeon.internetarchive.Item", return_value=mock_ia_item
        ):
            yield mock_ia
