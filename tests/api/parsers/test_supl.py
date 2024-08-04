import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.supl import SuplParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('supl.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('supl.parquet.gzip')


@pytest.fixture
def supl_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_supl():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def supl():
    yield SuplParser


@pytest.mark.private
def test_available_measurements(supl, supl_response, parsed_supl):
    resp = supl_response
    want = parsed_supl
    got = supl.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
