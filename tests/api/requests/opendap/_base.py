from datetime import datetime, timedelta

from tests._config import TESTS_DATA_DIR

REALTIME_START = datetime.today() - timedelta(days=30)
REALTIME_END = datetime.today()
HISTORICAL_START = datetime.strptime('2020-01-01', '%Y-%m-%d')
HISTORICAL_END = datetime.strptime('2021-07-15', '%Y-%m-%d')
REQUESTS_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'requests', 'opendap')
BASE_URL = 'https://www.ndbc.noaa.gov/'
