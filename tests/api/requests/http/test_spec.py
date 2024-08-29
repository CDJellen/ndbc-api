import pytest
import yaml

from ndbc_api.api.requests.http.spec import SpecRequest
from tests.api.requests.http._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('spec.yml')
TEST_STN = '41001'


@pytest.fixture
def spec():
    yield SpecRequest


@pytest.fixture
def spec_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def spec_realtime_requests(spec_requests):
    yield spec_requests.get('realtime')


@pytest.fixture
def spec_historical_requests(spec_requests):
    yield spec_requests.get('historical')


@pytest.mark.private
def test_spec_realtime(spec, spec_realtime_requests):
    want = spec_realtime_requests
    got = spec.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_spec_historical(spec, spec_historical_requests):
    want = spec_historical_requests
    got = spec.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
