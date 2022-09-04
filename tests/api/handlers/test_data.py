import logging
from os import path
from glob import glob
from typing import List

import pandas as pd
import httpretty
import pytest
import yaml

from ndbc_api.api.handlers.data import DataHandler
from ndbc_api.utilities.req_handler import RequestHandler
from ndbc_api.api.requests.adcp import AdcpRequest
from ndbc_api.api.requests.cwind import CwindRequest
from ndbc_api.api.requests.ocean import OceanRequest
from ndbc_api.api.requests.spec import SpecRequest
from ndbc_api.api.requests.stdmet import StdmetRequest
from ndbc_api.api.requests.supl import SuplRequest
from ndbc_api.api.requests.swden import SwdenRequest
from ndbc_api.api.requests.swdir import SwdirRequest
from ndbc_api.api.requests.swdir2 import Swdir2Request
from ndbc_api.api.requests.swr1 import Swr1Request
from ndbc_api.api.requests.swr2 import Swr2Request
from tests.api.handlers._base import (
    PARSED_TESTS_DIR,
    RESPONSES_TESTS_DIR,
    REQUESTS_TESTS_DIR,
    REALTIME_START,
    REALTIME_END,
    HISTORICAL_START,
    HISTORICAL_END,
)
from ndbc_api.exceptions import RequestException, ResponseException


REQUESTS_FP = REQUESTS_TESTS_DIR.joinpath('*.yml')
RESPONSES_FP = RESPONSES_TESTS_DIR.joinpath('*.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('*.parquet.gzip')
TEST_STN_ADCP = '41117'
TEST_STN_CWIND = 'TPLM2'
TEST_STN_OCEAN = '41024'
TEST_STN_SPEC = '41001'
TEST_STN_STDMET = 'TPLM2'
TEST_STN_SUPL = '41001'
TEST_STN_SWDEN = '41001'
TEST_STN_SWDIR = '41001'
TEST_STN_SWDIR2 = '41001'
TEST_STN_SWR1 = '41001'
TEST_STN_SWR2 = '41001'
TEST_LOG = logging.getLogger('TestDataHandler')


@pytest.fixture
def read_responses():
    resps = dict()

    for f in glob(str(RESPONSES_FP)):
        if 'station' not in f:
            name = path.basename(f).split('.')[0]
            with open(f, 'r') as f_yml:
                data = yaml.safe_load(f_yml)
            resps[name] = data

    yield resps


@pytest.fixture
def read_parsed():
    parsed = dict()

    for f in glob(str(PARSED_FP)):
        if 'station' not in f:
            name = path.basename(f).split('.')[0]
            data = pd.read_parquet(f)
            parsed[name] = data

    yield parsed


@pytest.fixture
def mock_socket():
    httpretty.enable(verbose=True, allow_net_connect=False)
    yield True
    httpretty.disable()
    httpretty.reset()


@pytest.fixture
def data_handler():
    yield DataHandler


@pytest.fixture
def request_handler():
    yield RequestHandler(
        cache_limit=10000, log=TEST_LOG, delay=1, retries=1, backoff_factor=0.5
    )


@pytest.mark.slow
def test_adcp(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = AdcpRequest.build_request(
        station_id=TEST_STN_ADCP,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['adcp'])
    mock_register_uri(reqs, read_responses['adcp'])
    want = read_parsed['adcp']
    got = data_handler.adcp(
        handler=request_handler,
        station_id=TEST_STN_ADCP,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.adcp(
            handler=request_handler,
            station_id=TEST_STN_ADCP,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.adcp(
            handler=None,
            station_id=TEST_STN_ADCP,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_cwind(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = CwindRequest.build_request(
        station_id=TEST_STN_CWIND,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['cwind'])
    mock_register_uri(reqs, read_responses['cwind'])
    want = read_parsed['cwind']
    got = data_handler.cwind(
        handler=request_handler,
        station_id=TEST_STN_CWIND,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.cwind(
            handler=request_handler,
            station_id=TEST_STN_CWIND,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.cwind(
            handler=None,
            station_id=TEST_STN_CWIND,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_ocean(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = OceanRequest.build_request(
        station_id=TEST_STN_OCEAN,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['ocean'])
    mock_register_uri(reqs, read_responses['ocean'])
    want = read_parsed['ocean']
    got = data_handler.ocean(
        handler=request_handler,
        station_id=TEST_STN_OCEAN,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.ocean(
            handler=request_handler,
            station_id=TEST_STN_OCEAN,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.ocean(
            handler=None,
            station_id=TEST_STN_OCEAN,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_spec(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = SpecRequest.build_request(
        station_id=TEST_STN_SPEC,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['spec'])
    mock_register_uri(reqs, read_responses['spec'])
    want = read_parsed['spec']
    got = data_handler.spec(
        handler=request_handler,
        station_id=TEST_STN_SPEC,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.spec(
            handler=request_handler,
            station_id=TEST_STN_SPEC,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.spec(
            handler=None,
            station_id=TEST_STN_SPEC,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_stdmet(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = StdmetRequest.build_request(
        station_id=TEST_STN_STDMET,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['stdmet'])
    mock_register_uri(reqs, read_responses['stdmet'])
    want = read_parsed['stdmet']
    got = data_handler.stdmet(
        handler=request_handler,
        station_id=TEST_STN_STDMET,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.stdmet(
            handler=request_handler,
            station_id=TEST_STN_STDMET,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.stdmet(
            handler=None,
            station_id=TEST_STN_STDMET,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_supl(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = SuplRequest.build_request(
        station_id=TEST_STN_SUPL,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['supl'])
    mock_register_uri(reqs, read_responses['supl'])
    want = read_parsed['supl']
    got = data_handler.supl(
        handler=request_handler,
        station_id=TEST_STN_SUPL,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END].sort_index(axis=1),
        got[HISTORICAL_START:HISTORICAL_END].sort_index(axis=1),
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.supl(
            handler=request_handler,
            station_id=TEST_STN_SUPL,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.supl(
            handler=None,
            station_id=TEST_STN_SUPL,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_swden(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = SwdenRequest.build_request(
        station_id=TEST_STN_SWDEN,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['swden'])
    mock_register_uri(reqs, read_responses['swden'])
    want = read_parsed['swden']
    got = data_handler.swden(
        handler=request_handler,
        station_id=TEST_STN_SWDEN,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.swden(
            handler=request_handler,
            station_id=TEST_STN_SWDEN,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.swden(
            handler=None,
            station_id=TEST_STN_SWDEN,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_swdir(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = SwdirRequest.build_request(
        station_id=TEST_STN_SWDIR,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['swdir'])
    mock_register_uri(reqs, read_responses['swdir'])
    want = read_parsed['swdir']
    got = data_handler.swdir(
        handler=request_handler,
        station_id=TEST_STN_SWDIR,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.swdir(
            handler=request_handler,
            station_id=TEST_STN_SWDIR,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.swdir(
            handler=None,
            station_id=TEST_STN_SWDIR,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_swdir2(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = Swdir2Request.build_request(
        station_id=TEST_STN_SWDIR2,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['swdir2'])
    mock_register_uri(reqs, read_responses['swdir2'])
    want = read_parsed['swdir2']
    got = data_handler.swdir2(
        handler=request_handler,
        station_id=TEST_STN_SWDIR2,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.swdir2(
            handler=request_handler,
            station_id=TEST_STN_ADCP,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.swdir2(
            handler=None,
            station_id=TEST_STN_SWDIR2,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_swr1(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = Swr1Request.build_request(
        station_id=TEST_STN_SWR1,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['swr1'])
    mock_register_uri(reqs, read_responses['swr1'])
    want = read_parsed['swr1']
    got = data_handler.swr1(
        handler=request_handler,
        station_id=TEST_STN_SWR1,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.swr1(
            handler=request_handler,
            station_id=TEST_STN_SWR1,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.swr1(
            handler=None,
            station_id=TEST_STN_SWR1,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


@pytest.mark.slow
def test_swr2(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    reqs = Swr2Request.build_request(
        station_id=TEST_STN_SWR2,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    assert len(reqs) == len(read_responses['swr2'])
    mock_register_uri(reqs, read_responses['swr2'])
    want = read_parsed['swr2']
    got = data_handler.swr2(
        handler=request_handler,
        station_id=TEST_STN_SWR2,
        start_time=HISTORICAL_START,
        end_time=HISTORICAL_END,
    )
    pd.testing.assert_frame_equal(
        want[HISTORICAL_START:HISTORICAL_END],
        got[HISTORICAL_START:HISTORICAL_END],
        check_dtype=False,
    )
    with pytest.raises(RequestException):
        _ = data_handler.swr2(
            handler=request_handler,
            station_id=TEST_STN_SWR2,
            start_time='foo',
            end_time='bar',
        )
    with pytest.raises(ResponseException) as e_info:
        _ = data_handler.swr2(
            handler=None,
            station_id=TEST_STN_SWR2,
            start_time=HISTORICAL_START,
            end_time=HISTORICAL_END,
        )


def mock_register_uri(
    read_requests: List[str], read_responses: List[str]
) -> None:
    for idx in range(len(read_requests)):
        httpretty.register_uri(
            httpretty.GET, read_requests[idx], body=read_responses[idx]['body']
        )
