import logging
from functools import partial

import pandas as pd
import pytest

from ndbc_api.api.handlers.http.stations import StationsHandler
from ndbc_api.api.requests.http.station_historical import HistoricalRequest
from ndbc_api.api.requests.http.station_metadata import MetadataRequest
from ndbc_api.api.requests.http.station_realtime import RealtimeRequest
from ndbc_api.api.requests.http.active_stations import ActiveStationsRequest
from ndbc_api.api.requests.http.historical_stations import HistoricalStationsRequest
from ndbc_api.exceptions import ResponseException
from ndbc_api.utilities.req_handler import RequestHandler
from tests.api.handlers._base import mock_register_uri

TEST_STN = 'TPLM2'
TEST_STN_REALTIME = '41013'
TEST_LOG = partial(logging.getLogger('TestDataHandler').log, msg="")


@pytest.fixture
def stations_handler():
    yield StationsHandler


@pytest.fixture(scope='module')
def request_handler():
    yield RequestHandler(cache_limit=10000,
                         log=TEST_LOG,
                         delay=1,
                         retries=1,
                         backoff_factor=0.5)


@pytest.fixture(autouse=True)
def _reset_request_handler_cache(request_handler):
    request_handler.stations.clear()
    yield
    request_handler.stations.clear()


@pytest.mark.private
@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_yml')
def test_station_meta(stations_handler, request_handler, read_parsed_yml,
                      read_responses, mock_socket):
    _ = mock_socket
    reqs = MetadataRequest.build_request(station_id=TEST_STN,)
    assert len([reqs]) == len(read_responses['metadata'].values())
    mock_register_uri([reqs], list(read_responses['metadata'].values()))
    want = read_parsed_yml['metadata']
    got = stations_handler.metadata(
        handler=request_handler,
        station_id=TEST_STN,
    )
    assert sorted(list(want.keys())) == sorted(list(got.keys()))
    with pytest.raises(ResponseException):
        _ = stations_handler.metadata(
            handler=None,
            station_id=TEST_STN,
        )


@pytest.mark.private
@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_yml')
def test_station_realtime(stations_handler, request_handler, read_parsed_yml,
                          read_responses, mock_socket):
    _ = mock_socket
    reqs = RealtimeRequest.build_request(station_id=TEST_STN,)
    assert len([reqs]) == len(read_responses['realtime'].values())
    mock_register_uri([reqs], list(read_responses['realtime'].values()))
    want = read_parsed_yml['realtime']
    got = stations_handler.realtime(
        handler=request_handler,
        station_id=TEST_STN_REALTIME,
    )
    assert want == got
    with pytest.raises(ResponseException):
        _ = stations_handler.realtime(
            handler=None,
            station_id=TEST_STN_REALTIME,
        )


@pytest.mark.private
@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_yml')
def test_station_historical(stations_handler, monkeypatch, request_handler,
                            read_parsed_yml, read_responses, mock_socket):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = HistoricalRequest.build_request(station_id=TEST_STN,)
    assert len([reqs]) == len(read_responses['historical'].values())
    mock_register_uri([reqs], list(read_responses['historical'].values()))
    want = read_parsed_yml['historical']
    got = stations_handler.historical(
        handler=request_handler,
        station_id=TEST_STN,
    )
    assert want == got
    with pytest.raises(ResponseException):
        _ = stations_handler.historical(
            handler=None,
            station_id=TEST_STN,
        )


@pytest.mark.slow
@pytest.mark.private
@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_df')
def test_stations(
    stations_handler,
    request_handler,
    read_responses,
    read_parsed_df,
    mock_socket,
):
    _ = mock_socket
    reqs = ActiveStationsRequest.build_request()
    assert len([reqs]) == len(read_responses['stations'].values())
    mock_register_uri([reqs], list(read_responses['stations'].values()))
    want = read_parsed_df['stations']
    got = stations_handler.stations(handler=request_handler,)
    got = pd.DataFrame(got)
    if 'station_id' in got.columns:
        got.set_index('station_id', inplace=True)
    pd.testing.assert_frame_equal(want, got, check_dtype=False)
    with pytest.raises(ResponseException):
        _ = stations_handler.stations(
            handler=None,
        )


@pytest.mark.private
@pytest.mark.usefixtures('mock_socket')
def test_historical_stations(stations_handler, request_handler, mock_socket):
    _ = mock_socket
    req = HistoricalStationsRequest.build_request()
    xml_body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<stations created="2023-01-01T00:00:00UTC" count="1">'
        '<station id="41001" name="EAST HATTERAS" owner="NDBC" pgm="NDBC" type="buoy">'
        '<history start="1976-06-02" stop="" lat="34.7" lng="-72.7" elev="0" met="y" hull="6N" anemom_height="5.0" />'
        '</station>'
        '</stations>'
    )
    mock_register_uri([req], [{'status': 200, 'body': xml_body}])
    got = stations_handler.historical_stations(handler=request_handler)
    assert isinstance(got, list)
    assert len(got) == 1
    assert got[0]['Station'] == '41001'
    with pytest.raises(ResponseException):
        _ = stations_handler.historical_stations(handler=None)


@pytest.mark.private
@pytest.mark.usefixtures('mock_socket')
def test_nearest_station_and_radial_search(stations_handler, request_handler, mock_socket):
    _ = mock_socket
    req = ActiveStationsRequest.build_request()
    xml_body = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<stations created="2023-01-01T00:00:00UTC" count="2">'
        '<station id="tplm2" lat="38.898" lon="-76.437" name="Thomas Point" owner="NOS" pgm="NOS" type="fixed" met="y" currents="n" waterquality="n" dart="n" />'
        '<station id="41001" lat="34.700" lon="-72.700" name="East Hatteras" owner="NDBC" pgm="NDBC" type="buoy" met="n" currents="y" waterquality="n" dart="n" />'
        '</stations>'
    )
    mock_register_uri([req], [{'status': 200, 'body': xml_body}])

    nearest = stations_handler.nearest_station(
        handler=request_handler, lat='38.88N', lon='76.43W'
    )
    assert nearest == 'tplm2'

    nearest_k2 = stations_handler.nearest_station(
        handler=request_handler, lat='38.88N', lon='76.43W', k=2
    )
    assert nearest_k2 == ['tplm2', '41001']

    nearest_currents = stations_handler.nearest_station(
        handler=request_handler, lat='38.88N', lon='76.43W', currents=True
    )
    assert nearest_currents == '41001'

    radial = stations_handler.radial_search(
        handler=request_handler,
        lat='38.88N',
        lon='76.43W',
        radius=50,
        units='nm',
        limit=1,
    )
    assert len(radial) == 1
    assert radial[0]['Station'] == 'tplm2'
    assert 'distance' in radial[0]
