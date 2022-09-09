from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class OceanRequest(BaseRequest):

    FORMAT = 'ocean'
    FILE_FORMAT = '.ocean'
    HISTORICAL_IDENTIFIER = 'o'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(OceanRequest, cls).build_request(station_id, start_time,
                                                      end_time)
