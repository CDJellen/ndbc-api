import pytest
import yaml
import pandas as pd

from ndbc_api.api.parsers.ocean import OceanParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR


TEST_FP = RESPONSES_TESTS_DIR.joinpath('ocean.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('ocean.parquet.gzip')


@pytest.fixture
def ocean_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_ocean():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def ocean():
    yield OceanParser


def test_available_measurements(ocean, ocean_response, parsed_ocean):
    resp = ocean_response
    want = parsed_ocean
    got = ocean.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got, want, check_dtype=False)
