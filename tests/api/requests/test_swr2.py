import pytest
import yaml

from ndbc_api.api.requests.swr2 import Swr2Request
from tests.api.requests._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('swr2.yml')
TEST_STN = '41001'


@pytest.fixture
def swr2():
    yield Swr2Request


@pytest.fixture
def swr2_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def swr2_realtime_requests(swr2_requests):
    yield swr2_requests.get('realtime')


@pytest.fixture
def swr2_historical_requests(swr2_requests):
    yield swr2_requests.get('historical')


@pytest.mark.private
def test_swr2_realtime(swr2, swr2_realtime_requests):
    want = swr2_realtime_requests
    got = swr2.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_swr2_historical(swr2, swr2_historical_requests):
    want = swr2_historical_requests
    got = swr2.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
