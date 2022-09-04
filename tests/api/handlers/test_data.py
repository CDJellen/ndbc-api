import logging
from os import path
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
    TEST_START,
    TEST_END,
    mock_register_uri,
)
from ndbc_api.exceptions import RequestException, ResponseException


REQUESTS_FP = REQUESTS_TESTS_DIR.glob('*.yml')
RESPONSES_FP = RESPONSES_TESTS_DIR.glob('*.yml')
PARSED_FP = PARSED_TESTS_DIR.glob('*.parquet.gzip')
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

    for f in RESPONSES_FP:
        if 'station' not in str(f):
            name = path.basename(str(f)).split('.')[0]
            with open(f, 'r') as f_yml:
                data = yaml.safe_load(f_yml)
            resps[name] = data

    yield resps


@pytest.fixture
def read_parsed():
    parsed = dict()

    for f in PARSED_FP:
        if 'station' not in str(f):
            name = path.basename(str(f)).split('.')[0]
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
def test_attrs(
    monkeypatch,
    data_handler,
    request_handler,
    read_responses,
    read_parsed,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    for name in [v for v in vars(data_handler) if not v.startswith('_')]:
        reqs = globals()[f'{name.capitalize()}Request'].build_request(
            station_id=globals()[f'TEST_STN_{name.upper()}'],
            start_time=TEST_START,
            end_time=TEST_END,
        )
        assert len(reqs) == len(read_responses[name])
        mock_register_uri(reqs, read_responses[name])
        want = read_parsed[name]
        got = getattr(data_handler, name)(
            handler=request_handler,
            station_id=globals()[f'TEST_STN_{name.upper()}'],
            start_time=TEST_START,
            end_time=TEST_END,
        )
        pd.testing.assert_frame_equal(
            want[TEST_START:TEST_END].sort_index(axis=1),
            got[TEST_START:TEST_END].sort_index(axis=1),
            check_dtype=False,
        )
        with pytest.raises(RequestException):
            _ = getattr(data_handler, name)(
                handler=request_handler,
                station_id=globals()[f'TEST_STN_{name.upper()}'],
                start_time='foo',
                end_time='bar',
            )
        with pytest.raises(ResponseException) as e_info:
            _ = getattr(data_handler, name)(
                handler=None,
                station_id=globals()[f'TEST_STN_{name.upper()}'],
                start_time=TEST_START,
                end_time=TEST_END,
            )
