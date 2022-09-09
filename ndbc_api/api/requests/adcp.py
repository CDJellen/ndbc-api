from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class AdcpRequest(BaseRequest):

    FORMAT = 'adcp'
    FILE_FORMAT = '.adcp'
    HISTORICAL_IDENTIFIER = 'a'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(AdcpRequest, cls).build_request(station_id, start_time,
                                                     end_time)
