import pytest
import yaml

from ndbc_api.api.requests.opendap.mmcbcur import MmcbcurRequest
from tests.api.requests.opendap._base import (HISTORICAL_END, HISTORICAL_START,
                                      REALTIME_END, REALTIME_START,
                                      REQUESTS_TESTS_DIR)

TEST_FP = REQUESTS_TESTS_DIR.joinpath('mmcbcur.yml')
TEST_STN = '41001'


@pytest.fixture
def mmcbcur():
    yield MmcbcurRequest


@pytest.fixture
def mmcbcur_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def mmcbcur_realtime_requests(mmcbcur_requests):
    yield mmcbcur_requests.get('realtime')


@pytest.fixture
def mmcbcur_historical_requests(mmcbcur_requests):
    yield mmcbcur_requests.get('historical')


@pytest.mark.private
def test_mmcbcur_realtime(mmcbcur, mmcbcur_realtime_requests):
    want = mmcbcur_realtime_requests
    got = mmcbcur.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got


@pytest.mark.private
def test_mmcbcur_historical(mmcbcur, mmcbcur_historical_requests):
    want = mmcbcur_historical_requests
    got = mmcbcur.build_request(TEST_STN, HISTORICAL_START, HISTORICAL_END)
    assert want == got
