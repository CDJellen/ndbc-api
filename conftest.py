from os import path

import httpretty
import pandas as pd
import pytest
import yaml

from tests.api.handlers._base import (PARSED_TESTS_DIR, REQUESTS_TESTS_DIR,
                                      RESPONSES_TESTS_DIR)

REQUESTS_FP = list(REQUESTS_TESTS_DIR.glob('*.yml'))
RESPONSES_FP = list(RESPONSES_TESTS_DIR.glob('*.yml'))
PARSED_YML_FP = list(PARSED_TESTS_DIR.glob('*.yml'))
PARSED_DF_FP = list(PARSED_TESTS_DIR.glob('*.parquet.gzip'))


def pytest_addoption(parser):
    parser.addoption('--run-slow',
                     action='store_true',
                     default=False,
                     help='run slow tests.')
    parser.addoption(
        '--run-private',
        action='store_true',
        default=False,
        help='run tests across methods which are not publicly exposed.')


def pytest_configure(config):
    config.addinivalue_line('markers', 'slow: mark test as slow to run')
    config.addinivalue_line('markers',
                            'private: mark test as not publicly exposed')


def pytest_collection_modifyitems(config, items):
    skips = set(['slow', 'private'])
    if config.getoption('--run-slow'):
        # --run-slow given in cli: do not skip slow tests
        skips.remove('slow')
    if config.getoption('--run-private'):
        # --run-private given in cli: do not skip private tests
        skips.remove('private')
    if not skips:
        return
    skip_slow = pytest.mark.skip(reason='need --run-slow option to run')
    skip_private = pytest.mark.skip(reason='need --run-private option to run')
    for item in items:
        if 'slow' in item.keywords and 'slow' in skips:
            item.add_marker(skip_slow)
        if 'private' in item.keywords and 'private' in skips:
            item.add_marker(skip_private)


@pytest.fixture(scope="session")
def read_responses():
    resps = dict()

    for f in RESPONSES_FP:
        name = path.basename(str(f)).split('.')[0].split('_')[-1]
        with open(f, 'r') as f_yml:
            data = yaml.safe_load(f_yml)
        resps[name] = data

    yield resps


@pytest.fixture(scope="session")
def read_parsed_yml():
    parsed = dict()

    for f in PARSED_YML_FP:
        name = path.basename(str(f)).split('.')[0].split('_')[-1]
        with open(f, 'r') as f_yml:
            data = yaml.safe_load(f_yml)
        parsed[name] = data

    yield parsed


@pytest.fixture(scope="session")
def read_parsed_df():
    parsed = dict()

    for f in PARSED_DF_FP:
        name = path.basename(str(f)).split('.')[0].split('_')[-1]
        data = pd.read_parquet(f)
        parsed[name] = data

    yield parsed


@pytest.fixture(scope="session")
def mock_socket():
    httpretty.enable(verbose=True, allow_net_connect=False)
    yield True
    httpretty.disable()
    httpretty.reset()
