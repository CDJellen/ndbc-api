from api.requests._core import CoreRequest


class StationMeasurementsRequest(CoreRequest):

    STATION_HISTORY_PREFIX = 'station_history.php?station='

    @classmethod
    def build_request(cls, station_id: str) -> str:
        return f'{cls.BASE_URL}{cls.STATION_REALTIME_PREFIX}{station_id}'
