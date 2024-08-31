import netCDF4 as nc

import pytest
import yaml

from ndbc_api.api.parsers.opendap.stdmet import StdmetParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('stdmet.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('stdmet.parquet.gzip')


@pytest.fixture
def stdmet_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_stdmet():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def stdmet():
    yield StdmetParser


@pytest.mark.private
def test_available_measurements(stdmet, stdmet_response, parsed_stdmet):
    resp = stdmet_response
    want = parsed_stdmet
    got = stdmet.nc_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
