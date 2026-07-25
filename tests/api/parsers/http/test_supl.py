import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.http.supl import SuplParser
from tests.api.parsers.http._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('supl.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('supl.parquet.gzip')


@pytest.fixture
def supl_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_supl():
    df = pd.read_parquet(PARSED_FP).where(lambda x: x.notna())
    yield df


@pytest.fixture
def supl():
    yield SuplParser


@pytest.mark.private
def test_available_measurements(supl, supl_response, parsed_supl):
    resp = supl_response
    want = parsed_supl
    got = pd.DataFrame(supl.parse_responses(resp, use_timestamp=True))
    if "timestamp" in got.columns:
        got.set_index("timestamp", inplace=True)
    assert isinstance(got, pd.DataFrame)
    assert set(got.columns) == set(want.columns)
