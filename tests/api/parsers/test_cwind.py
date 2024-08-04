import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.cwind import CwindParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('cwind.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('cwind.parquet.gzip')


@pytest.fixture
def cwind_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_cwind():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def cwind():
    yield CwindParser


@pytest.mark.private
def test_available_measurements(cwind, cwind_response, parsed_cwind):
    resp = cwind_response
    want = parsed_cwind
    got = cwind.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
