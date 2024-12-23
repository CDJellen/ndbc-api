import pytest
import yaml

from ndbc_api.api.requests.http.swdir2 import Swdir2Request
from tests.api.requests.http._base import (HISTORICAL_END, HISTORICAL_START,
                                           REALTIME_END, REALTIME_START,
                                           REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('swdir2.yml')
TEST_STN = '41001'


@pytest.fixture
def swdir2():
    yield Swdir2Request


@pytest.fixture
def swdir2_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def swdir2_realtime_requests(swdir2_requests):
    yield swdir2_requests.get('realtime')


@pytest.fixture
def swdir2_historical_requests(swdir2_requests):
    yield swdir2_requests.get('historical')


@pytest.mark.private
def test_swdir2_realtime(swdir2, swdir2_realtime_requests):
    want = swdir2_realtime_requests
    got = swdir2.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_swdir2_historical(swdir2, swdir2_historical_requests):
    want = swdir2_historical_requests
    got = swdir2.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
