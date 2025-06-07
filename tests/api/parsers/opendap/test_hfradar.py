import xarray

import pytest
import yaml

from ndbc_api.api.parsers.opendap.hfradar import HfradarParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('hfradar.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('hfradar.nc')


@pytest.fixture
def hfradar_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data


@pytest.fixture
def parsed_hfradar():
    ds = xarray.open_dataset(PARSED_FP)
    yield ds


@pytest.fixture
def hfradar():
    yield HfradarParser


@pytest.mark.private
def test_available_measurements(hfradar, hfradar_response, parsed_hfradar):
    resp = hfradar_response
    want = parsed_hfradar
    got = hfradar.nc_from_responses([resp], use_timestamp=True)

    assert set(want.variables.keys()).issubset(set(got.variables.keys()))
