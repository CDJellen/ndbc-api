import xarray

import pytest

from ndbc_api.api.parsers.opendap.stdmet import StdmetParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('stdmet.content')
PARSED_FP = PARSED_TESTS_DIR.joinpath('stdmet.nc')


@pytest.fixture
def stdmet_response():
    with open(TEST_FP, 'rb') as f:
        data = f.read()
    yield data


@pytest.fixture
def parsed_stdmet():
    ds = nc.Dataset(PARSED_FP, 'r')
    yield ds


@pytest.fixture
def stdmet():
    yield StdmetParser


@pytest.mark.private
def test_available_measurements(stdmet, stdmet_response, parsed_stdmet):
    resp = stdmet_response
    want = parsed_stdmet
    got = stdmet.nc_from_responses([resp], use_timestamp=True)

    assert set(want.variables.keys()).issubset(set(got.variables.keys()))
