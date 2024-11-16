import pytest
import yaml

from ndbc_api.api.requests.opendap.wlevel import WlevelRequest
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                              REALTIME_END, REALTIME_START,
                                              REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('wlevel.yml')
TEST_STN = '41001'


@pytest.fixture
def wlevel():
    yield WlevelRequest


@pytest.fixture
def wlevel_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def wlevel_realtime_requests(wlevel_requests):
    yield wlevel_requests.get('realtime')


@pytest.fixture
def wlevel_historical_requests(wlevel_requests):
    yield wlevel_requests.get('historical')


@pytest.mark.private
def test_wlevel_realtime(wlevel, wlevel_realtime_requests):
    want = wlevel_realtime_requests
    got = wlevel.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_wlevel_historical(wlevel, wlevel_historical_requests):
    want = wlevel_historical_requests
    got = wlevel.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
