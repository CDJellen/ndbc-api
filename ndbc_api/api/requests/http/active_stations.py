from ndbc_api.api.requests.http._core import CoreRequest


class ActiveStationsRequest(CoreRequest):

    STATIONS_URL = 'activestations.xml'

    @classmethod
    def build_request(cls) -> str:
        return f'{cls.BASE_URL}{cls.STATIONS_URL}'
