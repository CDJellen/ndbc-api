import xarray

import pytest

from ndbc_api.api.parsers.opendap.pwind import PwindParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('pwind.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('pwind.nc')


@pytest.fixture
def pwind_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data


@pytest.fixture
def parsed_pwind():
    ds = xarray.open_dataset(PARSED_FP)
    yield ds


@pytest.fixture
def pwind():
    yield PwindParser


@pytest.mark.private
def test_available_measurements(pwind, pwind_response, parsed_pwind):
    resp = pwind_response
    want = parsed_pwind
    got = pwind.nc_from_responses([resp], use_timestamp=True)

    assert set(want.variables.keys()).issubset(set(got.variables.keys()))
