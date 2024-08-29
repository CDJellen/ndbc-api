import xarray as xr
import pytest
import yaml

from ndbc_api.api.parsers.opendap.wlevel import WlevelParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('wlevel.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('wlevel.parquet.gzip')


@pytest.fixture
def wlevel_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_wlevel():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def wlevel():
    yield WlevelParser


@pytest.mark.private
def test_available_measurements(wlevel, wlevel_response, parsed_wlevel):
    resp = wlevel_response
    want = parsed_wlevel
    got = wlevel.xr_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)