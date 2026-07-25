import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.http.swdir2 import Swdir2Parser
from tests.api.parsers.http._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('swdir2.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('swdir2.parquet.gzip')


@pytest.fixture
def swdir2_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_swdir2():
    df = pd.read_parquet(PARSED_FP).where(lambda x: x.notna())
    yield df


@pytest.fixture
def swdir2():
    yield Swdir2Parser


@pytest.mark.private
def test_available_measurements(swdir2, swdir2_response, parsed_swdir2):
    resp = swdir2_response
    want = parsed_swdir2
    got = pd.DataFrame(swdir2.parse_responses(resp, use_timestamp=True))
    if "timestamp" in got.columns:
        got.set_index("timestamp", inplace=True)
    assert isinstance(got, pd.DataFrame)
    assert set(got.columns) == set(want.columns)
