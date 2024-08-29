import pytest

from ndbc_api.api.requests.http.historical_stations import HistoricalStationsRequest

STATIONS_URL = 'https://www.ndbc.noaa.gov/metadata/stationmetadata.xml'


@pytest.fixture
def stations():
    yield HistoricalStationsRequest


@pytest.mark.private
def test_stations_request(stations):
    want = STATIONS_URL
    got = stations.build_request()
    assert want == got
