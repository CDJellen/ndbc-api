from typing import List, Union

from api.requests._core import CoreRequest


class Stations(CoreRequest):

    STATION_REALTIME_PREFIX = 'station_realtime.php?station='
    STATION_HISTORY_PREFIX = 'station_history.php?station='

    @classmethod
    def build_requests(cls, station_id: str, mode: str = None
        ) -> Union[List[str], str]:
        if not mode:
            return [f'{cls.BASE_URL}{cls.STATION_REALTIME_PREFIX}{station_id}', f'{cls.BASE_URL}{cls.STATION_REALTIME_PREFIX}{station_id}']
        elif mode == 'realtime':
            return f'{cls.BASE_URL}{cls.STATION_REALTIME_PREFIX}{station_id}'
        elif mode == 'historical':
            return f'{cls.BASE_URL}{cls.STATION_REALTIME_PREFIX}{station_id}'
        else:
            raise NotImplementedError('Please supply one of `realtime` or `historical` if specifying a mode.')
