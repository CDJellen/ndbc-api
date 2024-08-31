import netCDF4 as nc

import pytest
import yaml

from ndbc_api.api.parsers.opendap.pwind import PwindParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('pwind.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('pwind.parquet.gzip')


@pytest.fixture
def pwind_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_pwind():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def pwind():
    yield PwindParser


@pytest.mark.private
def test_available_measurements(pwind, pwind_response, parsed_pwind):
    resp = pwind_response
    want = parsed_pwind
    got = pwind.nc_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
