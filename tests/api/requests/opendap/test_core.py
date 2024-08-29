import pytest

from ndbc_api.api.requests.opendap._core import CoreRequest
from tests.api.requests.opendap._base import BASE_URL


@pytest.fixture
def core():
    yield CoreRequest


@pytest.mark.private
def test_core_request_builder(core):
    want = BASE_URL
    got = core.build_request()
    assert want == got