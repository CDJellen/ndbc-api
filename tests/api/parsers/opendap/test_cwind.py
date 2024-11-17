import xarray

import pytest

from ndbc_api.api.parsers.opendap.cwind import CwindParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('cwind.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('cwind.nc')


@pytest.fixture
def cwind_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data


@pytest.fixture
def parsed_cwind():
    ds = xarray.open_dataset(PARSED_FP)
    yield ds


@pytest.fixture
def cwind():
    yield CwindParser


@pytest.mark.private
def test_available_measurements(cwind, cwind_response, parsed_cwind):
    resp = cwind_response
    want = parsed_cwind
    got = cwind.nc_from_responses([resp], use_timestamp=True)

    assert set(want.variables.keys()).issubset(set(got.variables.keys()))
