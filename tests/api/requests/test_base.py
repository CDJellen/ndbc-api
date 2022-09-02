import pytest

from ndbc_api.api.requests._base import BaseRequest
from tests.api.requests._base import HISTORICAL_START, REALTIME_END


TEST_STN = '41117'


@pytest.fixture
def base():
    yield BaseRequest


def test_base_request_builder(base):
    got = base.build_request(TEST_STN, HISTORICAL_START, REALTIME_END)
    assert isinstance(got, list)
    assert len(got) > 2
