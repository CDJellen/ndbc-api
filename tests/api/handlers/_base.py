from datetime import datetime, timedelta

from tests._config import TESTS_DATA_DIR


PARSED_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'parsed')
RESPONSES_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'responses')
REQUESTS_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'requests')
REALTIME_START = datetime.today() - timedelta(days=30)
REALTIME_END = datetime.today()
HISTORICAL_START = datetime.fromisoformat('2020-01-01')
HISTORICAL_END = datetime.fromisoformat('2022-07-15')
