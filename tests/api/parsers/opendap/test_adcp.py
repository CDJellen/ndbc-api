import netCDF4 as nc

import pytest
import yaml

from ndbc_api.api.parsers.opendap.adcp import AdcpParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('adcp.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('adcp.parquet.gzip')


@pytest.fixture
def adcp_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_adcp():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def adcp():
    yield AdcpParser


@pytest.mark.private
def test_available_measurements(adcp, adcp_response, parsed_adcp):
    resp = adcp_response
    want = parsed_adcp
    got = adcp.nc_from_responses(resp)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
