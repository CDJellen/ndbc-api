import pytest

from ndbc_api.api.requests.http._base import BaseRequest
from ndbc_api.api.requests.http.adcp import AdcpRequest
from tests.api.requests.http._base import HISTORICAL_START, REALTIME_END

TEST_STN = '41117'


@pytest.fixture
def adcp():
    yield AdcpRequest


@pytest.fixture
def base():
    yield BaseRequest


@pytest.mark.private
def test_base_request_builder(adcp):
    got = adcp.build_request(TEST_STN, HISTORICAL_START, REALTIME_END)
    assert isinstance(got, list)
    assert len(got) > 2


@pytest.mark.private
def test_base_fail(base):
    try:
        base.build_request(TEST_STN, HISTORICAL_START, REALTIME_END)
        raise AssertionError
    except ValueError:
        pass
