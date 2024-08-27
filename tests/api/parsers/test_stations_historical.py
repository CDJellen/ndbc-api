import pandas as pd
import pytest
import yaml

from ndbc_api.api.parsers.historical_stations import HistoricalStationsParser
from tests.api.parsers._base import PARSED_TESTS_DIR, RESPONSES_TESTS_DIR

TEST_FP = RESPONSES_TESTS_DIR.joinpath('stations_historical.yml')
PARSED_FP = PARSED_TESTS_DIR.joinpath('stations_historical.parquet.gzip')


@pytest.fixture
def stations_response():
    with open(TEST_FP, 'r') as f:
        data = yaml.safe_load(f)
    yield data


@pytest.fixture
def parsed_stations():
    df = pd.read_parquet(PARSED_FP)
    yield df


@pytest.fixture
def stations():
    yield HistoricalStationsParser


@pytest.mark.private
def test_df_from_responses(stations, stations_response, parsed_stations):
    resp = stations_response.get(list(stations_response.keys())[0])
    want = parsed_stations
    got = stations.df_from_response(resp)
    pd.testing.assert_frame_equal(want, got, check_dtype=False)
