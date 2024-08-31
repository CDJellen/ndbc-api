import netCDF4 as nc

import pytest
import yaml

from ndbc_api.api.parsers.opendap.mmcbcur import MmcbcurParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('mmcbcur.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('mmcbcur.parquet.gzip')


@pytest.fixture
def mmcbcur_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_mmcbcur():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def mmcbcur():
    yield MmcbcurParser


@pytest.mark.private
def test_available_measurements(mmcbcur, mmcbcur_response, parsed_mmcbcur):
    resp = mmcbcur_response
    want = parsed_mmcbcur
    got = mmcbcur.nc_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
