import logging
from functools import partial

import pandas as pd
import pytest

from ndbc_api.api.handlers.opendap.data import OpenDapDataHandler
from ndbc_api.api.requests.opendap.adcp import AdcpRequest
from ndbc_api.api.requests.opendap.cwind import CwindRequest
from ndbc_api.api.requests.opendap.ocean import OceanRequest
from ndbc_api.api.requests.opendap.stdmet import StdmetRequest
from ndbc_api.api.requests.opendap.swden import SwdenRequest
from ndbc_api.api.requests.opendap.pwind import PwindRequest
from ndbc_api.api.requests.opendap.wlevel import WlevelRequest
from ndbc_api.exceptions import RequestException, ResponseException
from ndbc_api.utilities.req_handler import RequestHandler
from tests.api.handlers._base import TEST_END, TEST_START

TEST_STN_ADCP = '41117'
TEST_STN_CWIND = 'TPLM2'
TEST_STN_OCEAN = '41024'
TEST_STN_PWIND = '41001'
TEST_STN_STDMET = 'TPLM2'
TEST_STN_SWDEN = '41001'
TEST_STN_WLEVEL = '41001'
TEST_LOG = partial(logging.getLogger('OpenDapDataHandler').log, msg="")


@pytest.fixture
def data_handler():
    yield OpenDapDataHandler


@pytest.fixture(scope='module')
def request_handler():
    yield RequestHandler(cache_limit=10000,
                         log=TEST_LOG,
                         delay=0.1,
                         retries=0,
                         backoff_factor=0.5)


@pytest.mark.slow
@pytest.mark.private
@pytest.mark.usefixtures('mock_socket')
def test_attrs(
    monkeypatch,
    data_handler,
    request_handler,
    mock_socket,
):
    _ = mock_socket
    monkeypatch.setenv('MOCKDATE', '2022-08-13')
    for name in [v for v in vars(data_handler) if not v.startswith('_')]:
        with pytest.raises(RequestException):
            _ = getattr(data_handler, name)(
                handler=request_handler,
                station_id=globals()[f'TEST_STN_{name.upper()}'],
                start_time='foo',
                end_time='bar',
            )
        with pytest.raises(ResponseException):
            _ = getattr(data_handler, name)(
                handler=None,
                station_id=globals()[f'TEST_STN_{name.upper()}'],
                start_time=TEST_START,
                end_time=TEST_END,
            )
