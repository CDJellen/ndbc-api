from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class Swr2Request(BaseRequest):

    FORMAT = 'swr2'
    FILE_FORMAT = '.swr2'
    HISTORICAL_IDENTIFIER = 'k'

    @classmethod
    def build_request(
        cls, station_id: str, start_time: datetime, end_time: datetime
    ) -> List[str]:
        return super(Swr2Request, cls).build_request(
            station_id, start_time, end_time
        )
