import pytest
import yaml

from ndbc_api.api.parsers.station_realtime import RealtimeParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('station_realtime.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('station_realtime.yml')


@pytest.fixture
def realtime_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_stations_realtime():
    with open(PARSED_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def stations_realtime():
    yield RealtimeParser


@pytest.mark.private
def test_available_measurements(stations_realtime, realtime_response,
                                parsed_stations_realtime):
    resp = realtime_response.get(list(realtime_response.keys())[0])
    want = parsed_stations_realtime
    got = stations_realtime.available_measurements(resp)
    assert want.keys() == got.keys()
    for k in want.keys():
        assert want[k] == got[k]


@pytest.mark.private
def test_available_measurements_status(stations_realtime, realtime_response):
    resp = realtime_response.get(list(realtime_response.keys())[0])
    resp['status'] = 404
    want = dict()
    got = stations_realtime.available_measurements(resp)
    assert want == got


@pytest.mark.private
def test_available_measurements_parse_item(stations_realtime,
                                           realtime_response):
    resp = realtime_response.get(list(realtime_response.keys())[0])
    resp['body'] = resp['body'].split('<ul>\n\t<li>')[0]
    want = dict()
    got = stations_realtime.available_measurements(resp)
    assert want == got
