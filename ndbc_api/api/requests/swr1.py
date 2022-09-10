from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class Swr1Request(BaseRequest):

    FORMAT = 'swr1'
    FILE_FORMAT = '.swr1'
    HISTORICAL_IDENTIFIER = 'j'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(Swr1Request, cls).build_request(station_id, start_time,
                                                     end_time)
