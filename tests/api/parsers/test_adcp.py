import pytest
import yaml
import pandas as pd

from ndbc_api.api.parsers.adcp import AdcpParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR


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
    got = adcp.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got, want, check_dtype=False)
