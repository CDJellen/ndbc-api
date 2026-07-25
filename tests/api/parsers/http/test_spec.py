import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.http.spec import SpecParser
from tests.api.parsers.http._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('spec.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('spec.parquet.gzip')


@pytest.fixture
def spec_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_spec():
    df = pd.read_parquet(PARSED_FP).where(lambda x: x.notna())
    yield df


@pytest.fixture
def spec():
    yield SpecParser


@pytest.mark.private
def test_available_measurements(spec, spec_response, parsed_spec):
    resp = spec_response
    want = parsed_spec
    got = pd.DataFrame(spec.parse_responses(resp, use_timestamp=True))
    if "timestamp" in got.columns:
        got.set_index("timestamp", inplace=True)
    assert isinstance(got, pd.DataFrame)
    assert set(got.columns) == set(want.columns)
