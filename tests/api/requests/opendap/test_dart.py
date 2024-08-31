import pytest
import yaml

from ndbc_api.api.requests.opendap.dart import DartRequest
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('dart.yml')
TEST_STN = '41420'


@pytest.fixture
def dart():
    yield DartRequest


@pytest.fixture
def dart_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def dart_realtime_requests(dart_requests):
    yield dart_requests.get('realtime')


@pytest.fixture
def dart_historical_requests(dart_requests):
    yield dart_requests.get('historical')


@pytest.mark.private
def test_dart_realtime(dart, dart_realtime_requests):
    want = dart_realtime_requests
    got = dart.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_dart_historical(dart, dart_historical_requests):
    want = dart_historical_requests
    got = dart.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
