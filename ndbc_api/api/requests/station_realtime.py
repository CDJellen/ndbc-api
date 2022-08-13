from ndbc_api.api.requests._core import CoreRequest


class RealtimeRequest(CoreRequest):

    STATION_REALTIME_PREFIX = 'station_realtime.php?station='

    @classmethod
    def build_request(cls, station_id: str) -> str:
        return f'{cls.BASE_URL}{cls.STATION_REALTIME_PREFIX}{station_id}'
