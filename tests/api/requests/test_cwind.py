import pytest
import yaml

from ndbc_api.api.requests.cwind import CwindRequest
from tests.api.requests._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('cwind.yml')
TEST_STN = 'tplm2'


@pytest.fixture
def cwind():
    yield CwindRequest


@pytest.fixture
def cwind_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def cwind_realtime_requests(cwind_requests):
    yield cwind_requests.get('realtime')


@pytest.fixture
def cwind_historical_requests(cwind_requests):
    yield cwind_requests.get('historical')


@pytest.mark.private
def test_cwind_realtime(cwind, cwind_realtime_requests):
    want = cwind_realtime_requests
    got = cwind.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_cwind_historical(cwind, cwind_historical_requests):
    want = cwind_historical_requests
    got = cwind.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
