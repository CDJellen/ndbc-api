import pytest
import yaml

from ndbc_api.api.requests.http.supl import SuplRequest
from tests.api.requests.http._base import (HISTORICAL_END, HISTORICAL_START,
                                           REALTIME_END, REALTIME_START,
                                           REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('supl.yml')
TEST_STN = '41001'


@pytest.fixture
def supl():
    yield SuplRequest


@pytest.fixture
def supl_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def supl_realtime_requests(supl_requests):
    yield supl_requests.get('realtime')


@pytest.fixture
def supl_historical_requests(supl_requests):
    yield supl_requests.get('historical')


@pytest.mark.private
def test_supl_realtime(supl, supl_realtime_requests):
    want = supl_realtime_requests
    got = supl.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_supl_historical(supl, supl_historical_requests):
    want = supl_historical_requests
    got = supl.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
