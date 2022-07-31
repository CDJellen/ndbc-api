from datetime import datetime
from typing import List

from api.requests._base import BaseRequest

class Swdir2(BaseRequest):

    FORMAT = 'swdir2'
    FILE_FORMAT = '.swdir2'

    @classmethod
    def build_request(
        cls,
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> List[str]:
        return super(Swdir2, cls).build_request(station_id, start_time, end_time)
