import pytest

from ndbc_api.api.requests._core import CoreRequest
from tests.api.requests._base import BASE_URL


@pytest.fixture
def core():
    yield CoreRequest


def test_core_request_builder(core):
    want = BASE_URL
    got = core.build_request()
    assert want == got