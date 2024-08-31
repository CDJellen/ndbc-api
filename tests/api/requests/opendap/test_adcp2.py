import pytest
import yaml

from ndbc_api.api.requests.opendap.adcp2 import Adcp2Request
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('adcp2.yml')
TEST_STN = '41001'


@pytest.fixture
def adcp2():
    yield Adcp2Request


@pytest.fixture
def adcp2_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def adcp2_realtime_requests(adcp2_requests):
    yield adcp2_requests.get('realtime')


@pytest.fixture
def adcp2_historical_requests(adcp2_requests):
    yield adcp2_requests.get('historical')


@pytest.mark.private
def test_adcp2_realtime(adcp2, adcp2_realtime_requests):
    want = adcp2_realtime_requests
    got = adcp2.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_adcp2_historical(adcp2, adcp2_historical_requests):
    want = adcp2_historical_requests
    got = adcp2.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
