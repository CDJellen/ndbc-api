import pytest

from ndbc_api.api.requests.station_realtime import RealtimeRequest


TEST_STN = '41001'
STATIONS_REALTIME_URL = (
    'https://www.ndbc.noaa.gov/station_realtime.php?station=41001'
)


@pytest.fixture
def stations_realtime():
    yield RealtimeRequest


def test_historical_request(stations_realtime):
    want = STATIONS_REALTIME_URL
    got = stations_realtime.build_request(TEST_STN)
    assert want == got
