import pytest
import yaml

from ndbc_api.api.requests.opendap.pwind import PwindRequest
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                              REALTIME_END, REALTIME_START,
                                              REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('pwind.yml')
TEST_STN = '41001'


@pytest.fixture
def pwind():
    yield PwindRequest


@pytest.fixture
def pwind_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def pwind_realtime_requests(pwind_requests):
    yield pwind_requests.get('realtime')


@pytest.fixture
def pwind_historical_requests(pwind_requests):
    yield pwind_requests.get('historical')


@pytest.mark.private
def test_pwind_realtime(pwind, pwind_realtime_requests):
    want = pwind_realtime_requests
    got = pwind.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_pwind_historical(pwind, pwind_historical_requests):
    want = pwind_historical_requests
    got = pwind.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
