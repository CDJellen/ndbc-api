from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class SwdirRequest(BaseRequest):

    FORMAT = 'swdir'
    FILE_FORMAT = '.swdir'
    HISTORICAL_IDENTIFIER = 'd'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(SwdirRequest, cls).build_request(station_id, start_time,
                                                      end_time)
