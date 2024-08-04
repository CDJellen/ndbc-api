import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.spec import SpecParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('spec.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('spec.parquet.gzip')


@pytest.fixture
def spec_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_spec():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def spec():
    yield SpecParser


@pytest.mark.private
def test_available_measurements(spec, spec_response, parsed_spec):
    resp = spec_response
    want = parsed_spec
    got = spec.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got,
                                  want,
                                  check_dtype=False,
                                  check_index_type=False)
