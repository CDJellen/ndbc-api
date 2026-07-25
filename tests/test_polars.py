"""Unit tests for Polars support in ndbc-api."""
import logging
import pytest

pytest.importorskip("polars")
import polars as pl
from unittest.mock import AsyncMock, MagicMock

from ndbc_api import NdbcApi, AsyncNdbcApi
from ndbc_api.api.requests.http.active_stations import ActiveStationsRequest
from ndbc_api.api.requests.http.station_metadata import MetadataRequest
from ndbc_api.api.requests.http.station_realtime import RealtimeRequest
from ndbc_api.api.requests.http.station_historical import HistoricalRequest
from ndbc_api.api.requests.http.historical_stations import HistoricalStationsRequest
from ndbc_api.api.requests.http.stdmet import StdmetRequest
from ndbc_api.utilities import data_helpers
from tests.api.handlers._base import mock_register_uri, TEST_START, TEST_END
from tests.test_ndbc_api import TEST_STN_STDMET, TEST_STN_REALTIME

@pytest.fixture
def ndbc_api():
    return NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)

@pytest.fixture
def async_api():
    api = AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
    handler_mock = MagicMock()
    handler_mock.handle_request = AsyncMock()
    handler_mock.handle_requests = AsyncMock()
    handler_mock.get_cache_limit = MagicMock(return_value=10000)
    handler_mock.set_cache_limit = MagicMock()
    handler_mock.get_headers = MagicMock(return_value={})
    handler_mock.update_headers = MagicMock()
    handler_mock.set_headers = MagicMock()
    handler_mock.stations = []
    api._handler = handler_mock
    return api

@pytest.mark.usefixtures('mock_socket', 'read_responses', 'read_parsed_df')
def test_stations_polars(ndbc_api, mock_socket, read_responses, read_parsed_df):
    _ = mock_socket
    reqs = ActiveStationsRequest.build_request()
    mock_register_uri([reqs], list(read_responses['stations'].values()))
    df = ndbc_api.stations(as_pl=True)
    assert isinstance(df, pl.DataFrame)
    assert 'station_id' in df.columns or 'Station' in df.columns

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_radial_search_polars(ndbc_api, mock_socket, read_responses):
    _ = mock_socket
    reqs = ActiveStationsRequest.build_request()
    mock_register_uri([reqs], list(read_responses['stations'].values()))
    df = ndbc_api.radial_search(lat=38.88, lon=-76.43, radius=1000, units='km', as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_station_polars(ndbc_api, mock_socket, read_responses):
    _ = mock_socket
    reqs = MetadataRequest.build_request(station_id=TEST_STN_STDMET)
    mock_register_uri([reqs], list(read_responses['metadata'].values()))
    df = ndbc_api.station(station_id=TEST_STN_STDMET, as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_available_realtime_polars(ndbc_api, mock_socket, read_responses):
    _ = mock_socket
    reqs = RealtimeRequest.build_request(station_id=TEST_STN_REALTIME)
    mock_register_uri([reqs], list(read_responses['realtime'].values()))
    df = ndbc_api.available_realtime(station_id=TEST_STN_REALTIME, full_response=True, as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_available_historical_polars(ndbc_api, mock_socket, read_responses):
    _ = mock_socket
    reqs = HistoricalRequest.build_request(station_id=TEST_STN_STDMET)
    mock_register_uri([reqs], list(read_responses['historical'].values()))
    df = ndbc_api.available_historical(station_id=TEST_STN_STDMET, as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_get_data_polars(ndbc_api, monkeypatch, mock_socket, read_responses):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = StdmetRequest.build_request(
        station_id=TEST_STN_STDMET,
        start_time=TEST_START,
        end_time=TEST_END,
    )
    mock_register_uri(reqs, read_responses['stdmet'])
    df = ndbc_api.get_data(
        station_id=TEST_STN_STDMET,
        mode='stdmet',
        start_time=TEST_START,
        end_time=TEST_END,
        as_pl=True
    )
    assert isinstance(df, pl.DataFrame)
    assert 'station_id' in df.columns
    assert 'timestamp' in df.columns

@pytest.mark.asyncio
async def test_async_stations_polars(async_api, read_responses):
    api = async_api
    resp_data = read_responses['stations']
    first_resp = list(resp_data.values())[0]
    api._handler.handle_request.return_value = first_resp
    df = await api.stations(as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.asyncio
async def test_async_radial_search_polars(async_api, read_responses):
    api = async_api
    resp_data = read_responses['stations']
    first_resp = list(resp_data.values())[0]
    api._handler.handle_request.return_value = first_resp
    df = await api.radial_search(lat=38.88, lon=-76.43, radius=1000, units='km', as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.asyncio
async def test_async_station_polars(async_api, read_responses):
    api = async_api
    resp_data = read_responses['metadata']
    first_resp = list(resp_data.values())[0]
    api._handler.handle_request.return_value = first_resp
    df = await api.station(station_id=TEST_STN_STDMET, as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.asyncio
async def test_async_available_realtime_polars(async_api, read_responses):
    api = async_api
    resp_data = read_responses['realtime']
    first_resp = list(resp_data.values())[0]
    api._handler.handle_request.return_value = first_resp
    df = await api.available_realtime(station_id=TEST_STN_REALTIME, full_response=True, as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.asyncio
async def test_async_available_historical_polars(async_api, read_responses):
    api = async_api
    resp_data = read_responses['historical']
    first_resp = list(resp_data.values())[0]
    api._handler.handle_request.return_value = first_resp
    df = await api.available_historical(station_id=TEST_STN_STDMET, as_pl=True)
    assert isinstance(df, pl.DataFrame)

@pytest.mark.asyncio
async def test_async_get_data_polars(async_api, monkeypatch, read_responses):
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    api = async_api
    resp_data = read_responses['stdmet']
    api._handler.handle_requests.return_value = resp_data
    df = await api.get_data(
        station_id=TEST_STN_STDMET,
        mode='stdmet',
        start_time=TEST_START,
        end_time=TEST_END,
        as_pl=True
    )
    assert isinstance(df, pl.DataFrame)

def test_polars_not_installed(monkeypatch, ndbc_api):
    monkeypatch.setattr(data_helpers, "pl", None)
    with pytest.raises(ImportError):
        ndbc_api.stations(as_pl=True)

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_get_data_polars_no_pandas(ndbc_api, monkeypatch, mock_socket, read_responses):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    
    # Mock pandas as not installed in both data_helpers and _base parser
    monkeypatch.setattr(data_helpers, "pd", None)
    from ndbc_api.api.parsers.http import _base
    monkeypatch.setattr(_base, "pd", None)
    
    reqs = StdmetRequest.build_request(
        station_id=TEST_STN_STDMET,
        start_time=TEST_START,
        end_time=TEST_END,
    )
    mock_register_uri(reqs, read_responses['stdmet'])
    df = ndbc_api.get_data(
        station_id=TEST_STN_STDMET,
        mode='stdmet',
        start_time=TEST_START,
        end_time=TEST_END,
        as_pl=True,
        as_df=False
    )
    assert isinstance(df, pl.DataFrame)
    assert 'station_id' in df.columns
    assert 'timestamp' in df.columns

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_historical_stations_polars(ndbc_api, mock_socket, read_responses):
    _ = mock_socket
    reqs = HistoricalStationsRequest.build_request()
    resp_data = read_responses.get('historical-stations') or read_responses.get('stations')
    mock_register_uri([reqs], list(resp_data.values()))
    df = ndbc_api.historical_stations(as_pl=True)
    assert isinstance(df, pl.DataFrame)
    if not df.is_empty():
        assert 'Station' in df.columns

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_get_data_polars_conflict(ndbc_api, monkeypatch, mock_socket, read_responses):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = StdmetRequest.build_request(
        station_id=TEST_STN_STDMET,
        start_time=TEST_START,
        end_time=TEST_END,
    )
    mock_register_uri(reqs, read_responses['stdmet'])
    # as_pl=True should override as_df=True
    df = ndbc_api.get_data(
        station_id=TEST_STN_STDMET,
        mode='stdmet',
        start_time=TEST_START,
        end_time=TEST_END,
        as_df=True,
        as_pl=True
    )
    assert isinstance(df, pl.DataFrame)

@pytest.mark.usefixtures('mock_socket', 'read_responses')
def test_get_data_polars_multi_station(ndbc_api, monkeypatch, mock_socket, read_responses):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = StdmetRequest.build_request(
        station_id=TEST_STN_STDMET,
        start_time=TEST_START,
        end_time=TEST_END,
    )
    mock_register_uri(reqs, read_responses['stdmet'])
    df = ndbc_api.get_data(
        station_ids=[TEST_STN_STDMET, TEST_STN_REALTIME],
        mode='stdmet',
        start_time=TEST_START,
        end_time=TEST_END,
        as_pl=True
    )
    assert isinstance(df, pl.DataFrame)
    assert 'station_id' in df.columns
    assert len(df['station_id'].unique()) > 0

@pytest.mark.asyncio
async def test_async_get_data_polars_no_pandas(async_api, monkeypatch, read_responses):
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    
    # Mock pandas as not installed in both data_helpers and _base parser
    monkeypatch.setattr(data_helpers, "pd", None)
    from ndbc_api.api.parsers.http import _base
    monkeypatch.setattr(_base, "pd", None)
    
    api = async_api
    resp_data = read_responses['stdmet']
    api._handler.handle_requests.return_value = resp_data
    df = await api.get_data(
        station_id=TEST_STN_STDMET,
        mode='stdmet',
        start_time=TEST_START,
        end_time=TEST_END,
        as_pl=True,
        as_df=False
    )
    assert isinstance(df, pl.DataFrame)
    assert 'station_id' in df.columns
    assert 'timestamp' in df.columns
