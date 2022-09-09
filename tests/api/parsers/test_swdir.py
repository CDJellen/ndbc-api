import pytest
import yaml
import pandas as pd

from ndbc_api.api.parsers.swdir import SwdirParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('swdir.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('swdir.parquet.gzip')


@pytest.fixture
def swdir_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_swdir():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def swdir():
    yield SwdirParser


@pytest.mark.private
def test_available_measurements(swdir, swdir_response, parsed_swdir):
    resp = swdir_response
    want = parsed_swdir
    got = swdir.df_from_responses(resp, use_timestamp=True)
    pd.testing.assert_frame_equal(got, want, check_dtype=False)
