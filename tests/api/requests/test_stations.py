import pytest

from ndbc_api.api.requests.stations import StationsRequest


STATIONS_URL = 'https://www.ndbc.noaa.gov/wstat.shtml'


@pytest.fixture
def stations():
    yield StationsRequest

@pytest.mark.private
def test_stations_request(stations):
    want = STATIONS_URL
    got = stations.build_request()
    assert want == got
