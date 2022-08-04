from datetime import datetime
from typing import List

from ndbc_api.api.requests._base import BaseRequest


class DataSpecRequest(BaseRequest):

    FORMAT = 'data_spec'
    FILE_FORMAT = '.data_spec'

    @classmethod
    def build_request(
        cls,
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> List[str]:
        return super(DataSpecRequest, cls).build_request(station_id, start_time, end_time)
