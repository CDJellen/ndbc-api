import xarray

import pytest

from ndbc_api.api.parsers.opendap.ocean import OceanParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('ocean.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('ocean.nc')


@pytest.fixture
def ocean_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data


@pytest.fixture
def parsed_ocean():
    ds = xarray.open_dataset(PARSED_FP, 'r')
    yield ds


@pytest.fixture
def ocean():
    yield OceanParser


@pytest.mark.private
def test_available_measurements(ocean, ocean_response, parsed_ocean):
    resp = ocean_response
    want = parsed_ocean
    got = ocean.nc_from_responses([resp], use_timestamp=True)

    assert set(want.variables.keys()).issubset(set(got.variables.keys()))
