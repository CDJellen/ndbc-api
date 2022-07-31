from api.requests._core import CoreRequest


class Stations(CoreRequest):

    STATIONS_URL = 'wstat.shtml'

    @classmethod
    def build_request(cls) -> str:
        return f'{cls.BASE_URL}{cls.STATIONS_URL}'
