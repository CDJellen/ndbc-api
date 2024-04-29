import logging
from datetime import datetime
from os import path

import pandas as pd
import pytest

from ndbc_api.api.requests.adcp import AdcpRequest
from ndbc_api.api.requests.cwind import CwindRequest
from ndbc_api.api.requests.ocean import OceanRequest
from ndbc_api.api.requests.spec import SpecRequest
from ndbc_api.api.requests.station_historical import HistoricalRequest
from ndbc_api.api.requests.station_metadata import MetadataRequest
from ndbc_api.api.requests.station_realtime import RealtimeRequest
from ndbc_api.api.requests.stations import StationsRequest
from ndbc_api.api.requests.stdmet import StdmetRequest
from ndbc_api.api.requests.supl import SuplRequest
from ndbc_api.api.requests.swden import SwdenRequest
from ndbc_api.api.requests.swdir import SwdirRequest
from ndbc_api.api.requests.swdir2 import Swdir2Request
from ndbc_api.api.requests.swr1 import Swr1Request
from ndbc_api.api.requests.swr2 import Swr2Request
from ndbc_api.exceptions import (HandlerException, ParserException,
                                 RequestException, ResponseException,
                                 TimestampException)
from ndbc_api.ndbc_api import NdbcApi
from tests.api.handlers._base import (PARSED_TESTS_DIR, TEST_END, TEST_START,
                                      mock_register_uri)

TEST_STN_ADCP = 41117
TEST_STN_CWIND = 'TPLM2'
TEST_STN_OCEAN = 41024
TEST_STN_SPEC = 41001
TEST_STN_STDMET = 'TPLM2'
TEST_STN_SUPL = 41001
TEST_STN_SWDEN = 41001
TEST_STN_SWDIR = 41001
TEST_STN_SWDIR2 = 41001
TEST_STN_SWR1 = 41001
TEST_STN_SWR2 = 41001
TEST_STN_REALTIME = 41013
TEST_CACHE_LIMIT = 10000
NEW_CACHE_LIMIT = 1000


@pytest.fixture
def ndbc_api():
    api = NdbcApi(cache_limit=TEST_CACHE_LIMIT)
    yield api


def test_init(ndbc_api):
    assert isinstance(ndbc_api, NdbcApi)
    assert isinstance(ndbc_api.log, logging.Logger)
    assert ndbc_api.cache_limit == TEST_CACHE_LIMIT
    assert NdbcApi() == ndbc_api


def test_clear_cache(ndbc_api):
    ndbc_api.cache_limit = NEW_CACHE_LIMIT
    ndbc_api.clear_cache()
    assert ndbc_api.cache_limit == NEW_CACHE_LIMIT


def test_set_cache_limit(ndbc_api):
    ndbc_api.set_cache_limit(TEST_CACHE_LIMIT)
    assert ndbc_api.get_cache_limit() == TEST_CACHE_LIMIT
    ndbc_api.set_cache_limit(NEW_CACHE_LIMIT)
    assert ndbc_api.get_cache_limit() == NEW_CACHE_LIMIT


def test_dump_cache_empty(ndbc_api):
    test_fp = None
    data = ndbc_api.dump_cache(dest_fp=test_fp)
    assert isinstance(data, dict)
    assert len(data) == len(ndbc_api._handler.stations)
    test_fp = PARSED_TESTS_DIR.joinpath('_dumped_cache.pickle')
    data = ndbc_api.dump_cache(dest_fp=test_fp)
    assert data is None
    assert path.exists(str(test_fp))
    test_fp.unlink()


@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_df')
def test_stations(ndbc_api, mock_socket, read_responses, read_parsed_df):
    _ = mock_socket
    reqs = StationsRequest.build_request()
    mock_register_uri([reqs], list(read_responses['stations'].values()))
    want = read_parsed_df['stations']
    got = ndbc_api.stations()
    assert isinstance(got, pd.DataFrame)
    pd.testing.assert_frame_equal(want, got)
    handler = ndbc_api._handler
    ndbc_api._handler = None
    with pytest.raises(Exception):
        _ = ndbc_api.stations()
    ndbc_api._handler = handler


def test_dump_cache_nonempty(ndbc_api):
    test_fp = None
    data = ndbc_api.dump_cache(dest_fp=test_fp)
    assert isinstance(data, dict)
    assert len(data) == len(ndbc_api._handler.stations)
    test_fp = PARSED_TESTS_DIR.joinpath('_dumped_cache.pickle')
    data = ndbc_api.dump_cache(dest_fp=test_fp)
    assert data is None
    assert path.exists(str(test_fp))
    test_fp.unlink()


def test_get_headers(ndbc_api):
    want = {}
    got = ndbc_api.get_headers()
    assert want == got


def test_update_headers(ndbc_api):
    current_headers = ndbc_api.get_headers()
    new_headers = {'foo': 'bar'}
    want = {**current_headers, **new_headers}
    ndbc_api.update_headers(new_headers)
    got = ndbc_api.get_headers()
    assert want == got


def test_set_headers(ndbc_api):
    new_headers = {'foo': 'bar'}
    want = new_headers
    ndbc_api.set_headers(new_headers)
    got = ndbc_api.get_headers()
    assert want == got


@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_yml')
def test_station(ndbc_api, mock_socket, read_responses, read_parsed_yml):
    _ = mock_socket
    reqs = MetadataRequest.build_request(station_id=TEST_STN_STDMET,)
    assert len([reqs]) == len(read_responses['metadata'].values())
    mock_register_uri([reqs], list(read_responses['metadata'].values()))
    want = read_parsed_yml['metadata']
    got = ndbc_api.station(station_id=TEST_STN_STDMET, as_df=False)
    assert want == got
    handler = ndbc_api._handler
    ndbc_api._handler = None
    with pytest.raises(Exception):
        _ = ndbc_api.station(station_id=TEST_STN_STDMET,)
    ndbc_api._handler = handler
    assert want == got
    with pytest.raises(Exception):
        _ = ndbc_api.nearest_station(lat=None, lon=None)
    with pytest.raises(Exception):
        _ = ndbc_api.nearest_station(lat=None, lon=None)
    want = 'TPLM2'
    got = ndbc_api.nearest_station(lat='38.88N', lon='76.43W')
    assert got == want
    got = ndbc_api.nearest_station(lat=38.88, lon=76.43)
    assert got == want
    with pytest.raises(Exception):
        _ = ndbc_api.radial_search(lat=None, lon=76.43, radius=100)
    with pytest.raises(Exception):
        _ = ndbc_api.nearest_station(lat='38.88N', lon='76.43W', radius=-100)
    with pytest.raises(Exception):
        _ = ndbc_api.nearest_station(lat='38.88N', lon='76.43W', radius=100, units='foo')
    got = ndbc_api.radial_search(lat='38.88N', lon='76.43W', radius=100)
    assert isinstance(got, pd.DataFrame)
    assert got.shape[0] > 0

@pytest.mark.slow
@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_df')
def test_get_data(ndbc_api, monkeypatch, mock_socket, read_responses,
                  read_parsed_df):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    for name in ndbc_api.get_modes():
        reqs = globals()[f'{name.capitalize()}Request'].build_request(
            station_id=str(globals()[f'TEST_STN_{name.upper()}']),
            start_time=TEST_START,
            end_time=TEST_END,
        )
        assert len(reqs) == len(read_responses[name])
        mock_register_uri(reqs, read_responses[name])
        want = read_parsed_df[name]
        got = ndbc_api.get_data(
            station_id=str(globals()[f'TEST_STN_{name.upper()}']),
            mode=name,
            start_time=TEST_START,
            end_time=TEST_END,
            use_timestamp=True,
            as_df=True,
            cols=None,
        )
        pd.testing.assert_frame_equal(
            want[TEST_START:TEST_END].sort_index(axis=1),
            got[TEST_START:TEST_END].sort_index(axis=1),
            check_dtype=False,
            check_index_type=False,
        )
        limited_cols = list(want.columns)[0:1]
        want = read_parsed_df[name][limited_cols]
        got = ndbc_api.get_data(
            station_id=str(globals()[f'TEST_STN_{name.upper()}']),
            mode=name,
            start_time=TEST_START,
            end_time=TEST_END,
            use_timestamp=True,
            as_df=True,
            cols=limited_cols,
        )
        pd.testing.assert_frame_equal(
            want[TEST_START:TEST_END].sort_index(axis=1),
            got[TEST_START:TEST_END].sort_index(axis=1),
            check_dtype=False,
            check_index_type=False,
        )
    handler = ndbc_api._handler
    ndbc_api._handler = None
    with pytest.raises(ResponseException):
        _ = ndbc_api.get_data(
            station_id=globals()[f'TEST_STN_{name.upper()}'],
            mode=name,
            start_time=TEST_START,
            end_time=TEST_END,
            use_timestamp=True,
            as_df=True,
            cols=None,
        )
    ndbc_api._handler = handler
    with pytest.raises(ParserException):
        _ = ndbc_api.get_data(
            station_id=globals()[f'TEST_STN_{name.upper()}'],
            mode=name,
            start_time=TEST_START,
            end_time=TEST_END,
            use_timestamp=True,
            as_df=True,
            cols=['FOOBAR'],
        )
    with pytest.raises(RequestException):
        _ = ndbc_api.get_data(
            station_id=globals()[f'TEST_STN_{name.upper()}'],
            mode='foo',
            start_time=TEST_START,
            end_time=TEST_END,
            use_timestamp=True,
            as_df=True,
            cols=None,
        )


def test_get_modes(ndbc_api):
    modes = ndbc_api.get_modes()
    assert isinstance(modes, list)
    assert len(modes) >= 1
    assert all([isinstance(v, str) for v in modes])


@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_yml')
def test_station_realtime(ndbc_api, monkeypatch, mock_socket, read_responses,
                          read_parsed_yml):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = RealtimeRequest.build_request(station_id=TEST_STN_REALTIME,)
    assert len([reqs]) == len(read_responses['realtime'].values())
    mock_register_uri([reqs], list(read_responses['realtime'].values()))
    want = read_parsed_yml['realtime']
    got = ndbc_api.available_realtime(
        station_id=TEST_STN_REALTIME,
        as_df=False,
    )
    assert want == got
    handler = ndbc_api._handler
    ndbc_api._handler = None
    with pytest.raises(Exception):
        _ = ndbc_api.available_realtime(
            station_id=TEST_STN_REALTIME,
            as_df=False,
        )
    ndbc_api._handler = handler


@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_yml')
def test_station_historical(ndbc_api, monkeypatch, mock_socket, read_responses,
                            read_parsed_yml):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = HistoricalRequest.build_request(station_id=TEST_STN_STDMET,)
    assert len([reqs]) == len(read_responses['historical'].values())
    mock_register_uri([reqs], list(read_responses['historical'].values()))
    want = read_parsed_yml['historical']
    got = ndbc_api.available_historical(
        station_id=TEST_STN_STDMET,
        as_df=False,
    )
    assert want == got
    handler = ndbc_api._handler
    ndbc_api._handler = None
    with pytest.raises(Exception):
        _ = ndbc_api.available_realtime(
            station_id=TEST_STN_REALTIME,
            as_df=False,
        )
    ndbc_api._handler = handler


@pytest.mark.private
def test_handle_timestamp(ndbc_api):
    test_convert_timestamp = '2020-01-01'
    test_converted_timestamp = datetime.strptime('2020-01-01', '%Y-%m-%d')
    want = test_converted_timestamp
    got = ndbc_api._handle_timestamp(test_convert_timestamp)
    assert got == want
    got = ndbc_api._handle_timestamp(test_converted_timestamp)
    assert got == want
    with pytest.raises(TimestampException):
        _ = ndbc_api._handle_timestamp('foo')


@pytest.mark.private
def test_enforce_timerange(ndbc_api, read_parsed_df):
    name = ndbc_api.get_modes()[-1]
    data = read_parsed_df[name]
    with pytest.raises(TimestampException):
        _ = ndbc_api._enforce_timerange(data, 'foo', 'bar')


@pytest.mark.private
def test_handle_data(ndbc_api, read_parsed_df):
    name = ndbc_api.get_modes()[-1]
    data = read_parsed_df[name]
    want = data.copy().T
    got = ndbc_api._handle_data(data.copy().to_dict(), as_df=True, cols=None)
    pd.testing.assert_frame_equal(want,
                                  got,
                                  check_dtype=False,
                                  check_names=False,
                                  check_index_type=False,
                                  check_column_type=False)
    with pytest.raises(HandlerException):
        _ = ndbc_api._handle_data(
            {
                'foo': ['bar', 'foobar'],
                'baz': {
                    'bar': 'bazbar'
                },
                'foobar': None
            },
            as_df=True,
            cols=None)
    want = data.copy().to_dict()
    got = ndbc_api._handle_data(data.copy(), as_df=False, cols=None)
    assert isinstance(want, dict)
    assert isinstance(got, dict)
    assert len(want.keys()) == len(got.keys())
