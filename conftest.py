import pytest


def pytest_addoption(parser):
    parser.addoption(
        '--run-slow', action='store_true', default=False, help='run slow tests.'
    )
    parser.addoption(
        '--run-private', action='store_true', default=False, help='run tests across methods which are not publicly exposed.'
    )

def pytest_configure(config):
    config.addinivalue_line('markers', 'slow: mark test as slow to run')
    config.addinivalue_line('markers', 'private: mark test as not publicly exposed')


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
