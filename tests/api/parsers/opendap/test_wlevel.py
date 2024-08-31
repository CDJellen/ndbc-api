import netCDF4 as nc

import pytest
import yaml

from ndbc_api.api.parsers.opendap.wlevel import WlevelParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('wlevel.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('wlevel.nc')


@pytest.fixture
def wlevel_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data



@pytest.fixture
def parsed_wlevel():
    ds = nc.Dataset(PARSED_FP, 'r')
    yield ds


@pytest.fixture
def wlevel():
    yield WlevelParser


@pytest.mark.private
def test_available_measurements(wlevel, wlevel_response, parsed_wlevel):
    resp = wlevel_response
    want = parsed_wlevel
    got = wlevel.nc_from_responses([resp], use_timestamp=True)
    assert set(want.variables.keys()) == set(got.variables.keys())
