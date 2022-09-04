import logging
from os import path

import pandas as pd
import httpretty
import pytest
import yaml

from ndbc_api.api.handlers.stations import StaitonsHandler
from ndbc_api.utilities.req_handler import RequestHandler
from ndbc_api.api.requests.stations import StationsRequest
from ndbc_api.api.requests.station_historical import HistoricalRequest
from ndbc_api.api.requests.station_realtime import RealtimeRequest
from ndbc_api.api.requests.station_metadata import MetadataRequest
from tests.api.handlers._base import (
    PARSED_TESTS_DIR,
    RESPONSES_TESTS_DIR,
    REQUESTS_TESTS_DIR,
    mock_register_uri,
)
from ndbc_api.exceptions import ResponseException


REQUESTS_FP = list(REQUESTS_TESTS_DIR.glob('*.yml'))
RESPONSES_FP = list(RESPONSES_TESTS_DIR.glob('*.yml'))
PARSED_FP = list(PARSED_TESTS_DIR.glob('*.yml'))
STATIONS_DF_FP = PARSED_TESTS_DIR.joinpath('stations.parquet.gzip')
TEST_STN = 'TPLM2'
TEST_STN_REALTIME = '41013'
TEST_LOG = logging.getLogger('TestDataHandler')


@pytest.fixture
def read_responses():
    resps = dict()

    for f in RESPONSES_FP:
        if 'station' in str(f):
            name = path.basename(str(f)).split('.')[0].split('_')[-1]
            with open(f, 'r') as f_yml:
                data = yaml.safe_load(f_yml)
            resps[name] = data

    yield resps


@pytest.fixture
def read_parsed():
    parsed = dict()

    for f in PARSED_FP:
        if 'station' in str(f):
            name = path.basename(str(f)).split('.')[0].split('_')[-1]
            with open(f, 'r') as f_yml:
                data = yaml.safe_load(f_yml)
            parsed[name] = data

    yield parsed


@pytest.fixture
def read_parsed_df():
    parsed = pd.read_parquet(STATIONS_DF_FP)
    yield parsed


@pytest.fixture
def mock_socket():
    httpretty.enable(verbose=True, allow_net_connect=False)
    yield True
    httpretty.disable()
    httpretty.reset()


@pytest.fixture
def stations_handler():
    yield StaitonsHandler


@pytest.fixture
def request_handler():
    yield RequestHandler(
        cache_limit=10000, log=TEST_LOG, delay=1, retries=1, backoff_factor=0.5
    )


def test_station_meta(
    stations_handler, request_handler, read_parsed, read_responses, mock_socket
):
    _ = mock_socket
    reqs = MetadataRequest.build_request(
        station_id=TEST_STN,
    )
    assert len([reqs]) == len(read_responses['metadata'].values())
    mock_register_uri([reqs], list(read_responses['metadata'].values()))
    want = read_parsed['metadata']
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


def test_station_realtime(
    stations_handler, request_handler, read_parsed, read_responses, mock_socket
):
    _ = mock_socket
    reqs = RealtimeRequest.build_request(
        station_id=TEST_STN,
    )
    assert len([reqs]) == len(read_responses['realtime'].values())
    mock_register_uri([reqs], list(read_responses['realtime'].values()))
    want = read_parsed['realtime']
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


def test_station_historical(
    stations_handler, request_handler, read_parsed, read_responses, mock_socket
):
    _ = mock_socket
    reqs = HistoricalRequest.build_request(
        station_id=TEST_STN,
    )
    assert len([reqs]) == len(read_responses['historical'].values())
    mock_register_uri([reqs], list(read_responses['historical'].values()))
    want = read_parsed['historical']
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
    want = read_parsed_df
    got = stations_handler.stations(
        handler=request_handler,
    )
    pd.testing.assert_frame_equal(want, got)
    with pytest.raises(ResponseException):
        _ = stations_handler.metadata(
            handler=None,
            station_id=TEST_STN,
        )
