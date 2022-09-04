from datetime import datetime, timedelta
from typing import List

import httpretty

from tests._config import TESTS_DATA_DIR


PARSED_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'parsed')
RESPONSES_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'responses')
REQUESTS_TESTS_DIR = TESTS_DATA_DIR.joinpath('api', 'requests')
TEST_START = datetime.fromisoformat('2020-01-01')
TEST_END = datetime.fromisoformat('2022-07-15')


def mock_register_uri(
    read_requests: List[str], read_responses: List[str]
) -> None:
    for idx in range(len(read_requests)):
        httpretty.register_uri(
            httpretty.GET, read_requests[idx], body=read_responses[idx]['body']
        )
