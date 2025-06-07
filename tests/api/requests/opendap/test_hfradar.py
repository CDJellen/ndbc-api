from datetime import timedelta
import pytest
import yaml

from ndbc_api.api.requests.opendap.hfradar import HfradarRequest
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                              REALTIME_END, REALTIME_START,
                                              REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('hfradar.yml')
TEST_STN = 'uswc_1km'


@pytest.fixture
def hfradar():
    yield HfradarRequest


@pytest.fixture
def hfradar_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def hfradar_realtime_requests(hfradar_requests):
    yield hfradar_requests.get('realtime')


@pytest.fixture
def hfradar_historical_requests(hfradar_requests):
    yield hfradar_requests.get('historical')


@pytest.mark.private
def test_hfradar_realtime(hfradar, hfradar_realtime_requests):
    want = hfradar_realtime_requests
    got = hfradar.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_START + timedelta(hours=1))  # we use historical start for testing
    assert want == got


@pytest.mark.private
def test_hfradar_historical(hfradar, hfradar_historical_requests):
    want = hfradar_historical_requests
    got = hfradar.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_START + timedelta(hours=1))
    assert want == got
