import pytest
import yaml

from ndbc_api.api.requests.http.swdir import SwdirRequest
from tests.api.requests.http._base import (HISTORICAL_END, HISTORICAL_START,
                                           REALTIME_END, REALTIME_START,
                                           REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('swdir.yml')
TEST_STN = '41001'


@pytest.fixture
def swdir():
    yield SwdirRequest


@pytest.fixture
def swdir_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def swdir_realtime_requests(swdir_requests):
    yield swdir_requests.get('realtime')


@pytest.fixture
def swdir_historical_requests(swdir_requests):
    yield swdir_requests.get('historical')


@pytest.mark.private
def test_swdir_realtime(swdir, swdir_realtime_requests):
    want = swdir_realtime_requests
    got = swdir.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_swdir_historical(swdir, swdir_historical_requests):
    want = swdir_historical_requests
    got = swdir.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
