import xarray as xr
import pytest
import yaml

from ndbc_api.api.parsers.opendap.ocean import OceanParser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('ocean.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('ocean.parquet.gzip')


@pytest.fixture
def ocean_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_ocean():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def ocean():
    yield OceanParser


@pytest.mark.private
def test_available_measurements(ocean, ocean_response, parsed_ocean):
    resp = ocean_response
    want = parsed_ocean
    got = ocean.xr_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
