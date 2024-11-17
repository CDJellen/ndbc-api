import xarray

import pytest

from ndbc_api.api.parsers.opendap.swden import SwdenParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('swden.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('swden.nc')


@pytest.fixture
def swden_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data


@pytest.fixture
def parsed_swden():
    ds = nc.Dataset(PARSED_FP, 'r')
    yield ds


@pytest.fixture
def swden():
    yield SwdenParser


@pytest.mark.private
def test_available_measurements(swden, swden_response, parsed_swden):
    resp = swden_response
    want = parsed_swden
    got = swden.nc_from_responses([resp], use_timestamp=True)

    assert set(want.variables.keys()).issubset(set(got.variables.keys()))
