import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.swden import SwdenParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('swden.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('swden.parquet.gzip')


@pytest.fixture
def swden_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_swden():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def swden():
    yield SwdenParser


@pytest.mark.private
def test_available_measurements(swden, swden_response, parsed_swden):
    resp = swden_response
    want = parsed_swden
    got = swden.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
