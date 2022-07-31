from datetime import datetime
from typing import List

from api.requests._base import BaseRequest


class Swr1(BaseRequest):

    FORMAT = 'swr1'
    FILE_FORMAT = '.swr1'

    @classmethod
    def build_request(
        cls,
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> List[str]:
        return super(Swr1, cls).build_request(station_id, start_time, end_time)
