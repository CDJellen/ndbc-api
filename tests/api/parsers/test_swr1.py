import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.swr1 import Swr1Parser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('swr1.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('swr1.parquet.gzip')


@pytest.fixture
def swr1_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_swr1():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def swr1():
    yield Swr1Parser


@pytest.mark.private
def test_available_measurements(swr1, swr1_response, parsed_swr1):
    resp = swr1_response
    want = parsed_swr1
    got = swr1.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got, want, check_dtype=False, check_index_type=False)
