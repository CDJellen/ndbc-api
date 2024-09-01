import pytest
import yaml

from ndbc_api.api.parsers.http.station_historical import HistoricalParser
from tests.api.parsers.http._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('station_historical.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('station_historical.yml')


@pytest.fixture
def historical_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_stations_historical():
    with open(PARSED_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def stations_historical():
    yield HistoricalParser


@pytest.mark.private
def test_available_measurements(stations_historical, monkeypatch,
                                historical_response,
                                parsed_stations_historical):
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    resp = historical_response.get(list(historical_response.keys())[0])
    want = parsed_stations_historical
    got = stations_historical.available_measurements(resp)
    assert want.keys() == got.keys()
    for k in want.keys():
        assert want[k] == got[k]


@pytest.mark.private
def test_available_measurements_status(stations_historical,
                                       historical_response):
    resp = historical_response.get(list(historical_response.keys())[0])
    resp['status'] = 404
    want = dict()
    got = stations_historical.available_measurements(resp)
    assert want == got


@pytest.mark.private
def test_available_measurements_parse_item(stations_historical,
                                           historical_response):
    resp = historical_response.get(list(historical_response.keys())[0])
    resp['body'] = resp['body'].split('HISTORY')[0]
    want = dict()
    got = stations_historical.available_measurements(resp)
    assert want == got
