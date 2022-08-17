import pytest
import yaml

from ndbc_api.api.parsers.station_metadata import MetadataParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR


TEST_FP = RESPONSES_TESTS_DIR.joinpath('station_metadata.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('station_metadata.yml')


@pytest.fixture
def metadata_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_stations_metadata():
    with open(PARSED_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def stations_metadata():
    yield MetadataParser


def test_station_metadata(
    stations_metadata, metadata_response, parsed_stations_metadata
):
    resp = metadata_response.get(list(metadata_response.keys())[0])
    want = parsed_stations_metadata
    got = stations_metadata.metadata(resp)
    assert want == got

def test_station_metadata_status(
    stations_metadata, metadata_response):
    resp = metadata_response.get(list(metadata_response.keys())[0])
    resp['status'] = 404
    want = dict()
    got = stations_metadata.available_measurements(resp)
    assert want == got
