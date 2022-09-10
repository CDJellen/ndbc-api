import pytest
import yaml
import pandas as pd

from ndbc_api.api.parsers.swr2 import Swr2Parser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('swr2.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('swr2.parquet.gzip')


@pytest.fixture
def swr2_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_swr2():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def swr2():
    yield Swr2Parser


@pytest.mark.private
def test_available_measurements(swr2, swr2_response, parsed_swr2):
    resp = swr2_response
    want = parsed_swr2
    got = swr2.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got, want, check_dtype=False)
