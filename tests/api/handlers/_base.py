from datetime import datetime, timedelta
from typing import List

import httpretty

from tests._config import TESTS_DATA_DIR

PARSED_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'parsed')
RESPONSES_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'responses')
REQUESTS_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'requests')
TEST_START = datetime.strptime('2020-01-01', '%Y-%m-%d')
TEST_END = datetime.strptime('2022-07-15', '%Y-%m-%d')


def mock_register_uri(read_requests: List[str],
                      read_responses: List[str]) -> None:
    for idx in range(len(read_requests)):
        httpretty.register_uri(httpretty.GET,
                               read_requests[idx],
                               body=read_responses[idx]['body'])
