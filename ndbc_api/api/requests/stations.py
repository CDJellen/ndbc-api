from ndbc_api.api.requests._core import CoreRequest


class StationsRequest(CoreRequest):

    STATIONS_URL = 'wstat.shtml'

    @classmethod
    def build_request(cls) -> str:
        return f'{cls.BASE_URL}{cls.STATIONS_URL}'
