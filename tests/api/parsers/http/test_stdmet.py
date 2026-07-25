import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.http.stdmet import StdmetParser
from tests.api.parsers.http._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('stdmet.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('stdmet.parquet.gzip')


@pytest.fixture
def stdmet_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_stdmet():
    df = pd.read_parquet(PARSED_FP).where(lambda x: x.notna())
    yield df


@pytest.fixture
def stdmet():
    yield StdmetParser


@pytest.mark.private
def test_available_measurements(stdmet, stdmet_response, parsed_stdmet):
    resp = stdmet_response
    want = parsed_stdmet
    got = pd.DataFrame(stdmet.parse_responses(resp, use_timestamp=True))
    if "timestamp" in got.columns:
        got.set_index("timestamp", inplace=True)
    assert isinstance(got, pd.DataFrame)
    assert set(got.columns) == set(want.columns)
