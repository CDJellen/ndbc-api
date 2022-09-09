from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class StdmetRequest(BaseRequest):

    FORMAT = 'stdmet'
    FILE_FORMAT = '.txt'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(StdmetRequest, cls).build_request(station_id, start_time,
                                                       end_time)
