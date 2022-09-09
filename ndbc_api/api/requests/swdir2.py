from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class Swdir2Request(BaseRequest):

    FORMAT = 'swdir2'
    FILE_FORMAT = '.swdir2'
    HISTORICAL_IDENTIFIER = 'i'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(Swdir2Request, cls).build_request(station_id, start_time,
                                                       end_time)
