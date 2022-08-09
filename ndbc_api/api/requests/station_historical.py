from ndbc_api.api.requests._core import CoreRequest


class HistoricalRequest(CoreRequest):

    STATION_HISTORY_PREFIX = 'station_history.php?station='

    @classmethod
    def build_request(cls, station_id: str) -> str:
        return f'{cls.BASE_URL}{cls.STATION_HISTORY_PREFIX}{station_id}'
