from datetime import datetime
from typing import List

from ndbc_api.api.requests.opendap._base import BaseRequest


class DartRequest(BaseRequest):

    FORMAT = 'dart'
    HISTORICAL_IDENTIFIER = 't'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        return super(DartRequest, cls).build_request(station_id, start_time,
                                                     end_time)
