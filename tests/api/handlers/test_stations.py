import logging

import pandas as pd
import pytest

from ndbc_api.api.handlers.stations import StationsHandler
from ndbc_api.utilities.req_handler import RequestHandler
from ndbc_api.api.requests.stations import StationsRequest
from ndbc_api.api.requests.station_historical import HistoricalRequest
from ndbc_api.api.requests.station_realtime import RealtimeRequest
from ndbc_api.api.requests.station_metadata import MetadataRequest
from tests.api.handlers._base import mock_register_uri
from ndbc_api.exceptions import ResponseException

TEST_STN = 'TPLM2'
TEST_STN_REALTIME = '41013'
TEST_LOG = logging.getLogger('TestStationsHandler')


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
    assert want == got
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
def test_station_historical(stations_handler, request_handler, read_parsed_yml,
                            read_responses, mock_socket):
    _ = mock_socket
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
    reqs = StationsRequest.build_request()
    assert len([reqs]) == len(read_responses['stations'].values())
    mock_register_uri([reqs], list(read_responses['stations'].values()))
    want = read_parsed_df['stations']
    got = stations_handler.stations(handler=request_handler,)
    pd.testing.assert_frame_equal(want, got)
    with pytest.raises(ResponseException):
        _ = stations_handler.metadata(
            handler=None,
            station_id=TEST_STN,
        )
