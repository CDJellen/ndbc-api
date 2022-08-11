import yaml

import pytest

from ndbc_api.api.requests.data_spec import DataSpecRequest
from tests.api.requests._base import REALTIME_START, REALTIME_END, REQUESTS_TESTS_DIR


TEST_FP = REQUESTS_TESTS_DIR.joinpath('data_spec.yml')
TEST_STN = '41013'

@pytest.fixture
def data_spec():
    yield DataSpecRequest

@pytest.fixture
def data_spec_requests():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data

@pytest.fixture
def data_spec_realtime_requests(data_spec_requests):
    yield data_spec_requests.get('realtime')

def test_data_spec_realtime(data_spec, data_spec_realtime_requests):
    want = data_spec_realtime_requests
    got = data_spec.build_request(TEST_STN, REALTIME_START, REALTIME_END)
    assert want == got
