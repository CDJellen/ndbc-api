import pytest

from ndbc_api.api.requests.http.station_historical import HistoricalRequest

TEST_STN = '41001'
STATIONS_HISTORICAL_URL = (
    'https://www.ndbc.noaa.gov/station_history.php?station=41001')


@pytest.fixture
def stations_historical():
    yield HistoricalRequest


@pytest.mark.private
def test_historical_request(stations_historical):
    want = STATIONS_HISTORICAL_URL
    got = stations_historical.build_request(TEST_STN)
    assert want == got
