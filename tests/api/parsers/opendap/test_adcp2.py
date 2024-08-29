import xarray as xr
import pytest
import yaml

from ndbc_api.api.parsers.opendap.adcp2 import Adcp2Parser
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('adcp2.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('adcp2.parquet.gzip')


@pytest.fixture
def adcp2_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_adcp2():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def adcp2():
    yield Adcp2Parser


@pytest.mark.private
def test_available_measurements(adcp2, adcp2_response, parsed_adcp2):
    resp = adcp2_response
    want = parsed_adcp2
    got = adcp2.xr_from_responses(resp)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
