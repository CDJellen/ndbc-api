from api.requests._core import CoreRequest


class Stations(CoreRequest):

    STATION_PREFIX = 'station_page.php?station='

    @classmethod
    def build_request(cls, station_id: str) -> str:
        return f'{cls.BASE_URL}{cls.STATION_PREFIX}{station_id}'
