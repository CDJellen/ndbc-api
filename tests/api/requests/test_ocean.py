import yaml

import pytest

from ndbc_api.api.requests.ocean import OceanRequest
from tests.api.requests._base import (
    REALTIME_START,
    REALTIME_END,
    HISTORICAL_START,
    HISTORICAL_END,
    REQUESTS_TESTS_DIR,
)


TEST_FP = REQUESTS_TESTS_DIR.joinpath('ocean.yml')
TEST_STN = '41024'


@pytest.fixture
def ocean():
    yield OceanRequest


@pytest.fixture
def ocean_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def ocean_realtime_requests(ocean_requests):
    yield ocean_requests.get('realtime')


@pytest.fixture
def ocean_historical_requests(ocean_requests):
    yield ocean_requests.get('historical')


def test_ocean_realtime(ocean, ocean_realtime_requests):
    want = ocean_realtime_requests
    got = ocean.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


def test_ocean_historical(ocean, ocean_historical_requests):
    want = ocean_historical_requests
    got = ocean.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
