import pytest
import yaml

from ndbc_api.api.requests.opendap.mmbcur import mmbcurRequest
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('mmbcur.yml')
TEST_STN = '41021'


@pytest.fixture
def mmbcur():
    yield mmbcurRequest


@pytest.fixture
def mmbcur_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def mmbcur_realtime_requests(mmbcur_requests):
    yield mmbcur_requests.get('realtime')


@pytest.fixture
def mmbcur_historical_requests(mmbcur_requests):
    yield mmbcur_requests.get('historical')


@pytest.mark.private
def test_mmbcur_realtime(mmbcur, mmbcur_realtime_requests):
    want = mmbcur_realtime_requests
    got = mmbcur.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_mmbcur_historical(mmbcur, mmbcur_historical_requests):
    want = mmbcur_historical_requests
    got = mmbcur.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
