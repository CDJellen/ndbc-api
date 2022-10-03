import pytest
import yaml

from ndbc_api.api.requests.swr1 import Swr1Request
from tests.api.requests._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('swr1.yml')
TEST_STN = '41001'


@pytest.fixture
def swr1():
    yield Swr1Request


@pytest.fixture
def swr1_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def swr1_realtime_requests(swr1_requests):
    yield swr1_requests.get('realtime')


@pytest.fixture
def swr1_historical_requests(swr1_requests):
    yield swr1_requests.get('historical')


@pytest.mark.private
def test_swr1_realtime(swr1, swr1_realtime_requests):
    want = swr1_realtime_requests
    got = swr1.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_swr1_historical(swr1, swr1_historical_requests):
    want = swr1_historical_requests
    got = swr1.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
