import yaml

import pytest

from ndbc_api.api.requests.swden import SwdenRequest
from tests.api.requests._base import (
    REALTIME_START,
    REALTIME_END,
    HISTORICAL_START,
    HISTORICAL_END,
    REQUESTS_TESTS_DIR,
)


TEST_FP = REQUESTS_TESTS_DIR.joinpath('swden.yml')
TEST_STN = '41001'


@pytest.fixture
def swden():
    yield SwdenRequest


@pytest.fixture
def swden_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def swden_realtime_requests(swden_requests):
    yield swden_requests.get('realtime')


@pytest.fixture
def swden_historical_requests(swden_requests):
    yield swden_requests.get('historical')


def test_swden_realtime(swden, swden_realtime_requests):
    want = swden_realtime_requests
    got = swden.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


def test_swden_historical(swden, swden_historical_requests):
    want = swden_historical_requests
    got = swden.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
