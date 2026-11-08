from functools import partial
from os import path

import httpretty
import pandas as pd
import pytest
import yaml

from tests.api.handlers._base import (PARSED_TESTS_DIR, REQUESTS_TESTS_DIR,
                                      RESPONSES_TESTS_DIR)

_YAML_LOADER = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
if hasattr(yaml, 'CSafeLoader'):
    yaml.safe_load = partial(yaml.load, Loader=_YAML_LOADER)

REQUESTS_FP = list(REQUESTS_TESTS_DIR.glob('*.yml'))
RESPONSES_FP = list(RESPONSES_TESTS_DIR.glob('*.yml'))
PARSED_YML_FP = list(PARSED_TESTS_DIR.glob('*.yml'))
PARSED_DF_FP = list(PARSED_TESTS_DIR.glob('*.parquet.gzip'))


class _LazyFixtureDict(dict):
    """Dictionary that loads fixture files lazily on first access and caches them."""

    def __init__(self, file_map, loader):
        super().__init__()
        self._file_map = dict(file_map)
        self._loader = loader

    def __contains__(self, key):
        return super().__contains__(key) or key in self._file_map

    def __getitem__(self, key):
        if not super().__contains__(key) and key in self._file_map:
            super().__setitem__(key, self._loader(self._file_map[key]))
        return super().__getitem__(key)

    def get(self, key, default=None):
        if key in self:
            return self[key]
        return default

    def keys(self):
        return self._file_map.keys()

    def __iter__(self):
        return iter(self._file_map)

    def __len__(self):
        return len(self._file_map)


def _load_yaml(fp):
    with open(fp, 'r') as f_yml:
        return yaml.load(f_yml, Loader=_YAML_LOADER)


def _load_parquet(fp):
    data = pd.read_parquet(fp)
    return data.where(data.notna())


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
    file_map = {
        path.basename(str(f)).split('.')[0].split('_')[-1]: f
        for f in RESPONSES_FP
    }
    yield _LazyFixtureDict(file_map, _load_yaml)


@pytest.fixture(scope="session")
def read_parsed_yml():
    file_map = {
        path.basename(str(f)).split('.')[0].split('_')[-1]: f
        for f in PARSED_YML_FP
    }
    yield _LazyFixtureDict(file_map, _load_yaml)


@pytest.fixture(scope="session")
def read_parsed_df():
    file_map = {
        path.basename(str(f)).split('.')[0].split('_')[-1]: f
        for f in PARSED_DF_FP
    }
    yield _LazyFixtureDict(file_map, _load_parquet)


@pytest.fixture(scope="function")
def mock_socket():
    httpretty.enable(verbose=True, allow_net_connect=False)
    yield True
    httpretty.disable()
    httpretty.reset()
