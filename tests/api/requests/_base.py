from datetime import datetime, timedelta

from tests._config import TESTS_DATA_DIR


REALTIME_START = datetime.today() - timedelta(days=30)
REALTIME_END = datetime.today()
HISTORICAL_START = datetime.fromisoformat('2020-01-01')
HISTORICAL_END = datetime.fromisoformat('2021-07-15')
REQUESTS_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'requests')
BASE_URL = 'https://www.ndbc.noaa.gov/'
