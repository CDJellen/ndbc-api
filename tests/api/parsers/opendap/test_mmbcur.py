import netCDF4 as nc

import pytest
import yaml

from ndbc_api.api.parsers.opendap.mmbcur import mmbcurParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('mmbcur.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('mmbcur.parquet.gzip')


@pytest.fixture
def mmbcur_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_mmbcur():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def mmbcur():
    yield mmbcurParser


@pytest.mark.private
def test_available_measurements(mmbcur, mmbcur_response, parsed_mmbcur):
    resp = mmbcur_response
    want = parsed_mmbcur
    got = mmbcur.nc_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
