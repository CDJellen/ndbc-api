from calendar import month_abbr
from datetime import datetime, timedelta
from typing import List

from ndbc_api.api.requests._core import CoreRequest


class BaseRequest(CoreRequest):

    REAL_TIME_URL_PREFIX = 'data/realtime2/'
    HISTORICAL_FILE_EXTENSION_SUFFIX = '.txt.gz'
    HISTORICAL_DATA_PREFIX = '&dir=data/'
    HISTORICAL_URL_PREFIX = 'view_text_file.php?filename='
    HISTORICAL_SUFFIX = 'historical/'
    HISTORICAL_IDENTIFIER = 'h'
    FORMAT = ''
    FILE_FORMAT = ''

    @classmethod
    def build_request(
        cls, station_id: str, start_time: datetime, end_time: datetime
    ) -> List[str]:

        is_historical = (datetime.now() - start_time) >= timedelta(days=44)
        if is_historical:
            return cls._build_request_historical(
                station_id=station_id, start_time=start_time, end_time=end_time
            )
        return cls._build_request_realtime(station_id=station_id)

    @classmethod
    def _build_request_historical(
        cls, station_id: str, start_time: datetime, end_time: datetime
    ) -> List[str]:
        def req_hist_helper_year(req_year: int) -> str:
            return f'{cls.BASE_URL}{cls.HISTORICAL_URL_PREFIX}{station_id}{cls.HISTORICAL_IDENTIFIER}{req_year}{cls.HISTORICAL_FILE_EXTENSION_SUFFIX}{cls.HISTORICAL_DATA_PREFIX}{cls.HISTORICAL_SUFFIX}{cls.FORMAT}/'

        def req_hist_helper_month(req_month: int) -> str:
            month = month_abbr[req_month]
            month = month.capitalize()
            return f'{cls.BASE_URL}{cls.HISTORICAL_URL_PREFIX}{station_id}{req_month}{current_year}{cls.HISTORICAL_FILE_EXTENSION_SUFFIX}{cls.HISTORICAL_DATA_PREFIX}{cls.FORMAT}/{month}/'

        def req_hist_helper_month_current(current_month: int) -> str:
            month = month_abbr[current_month]
            month = month.capitalize()
            return f'{cls.BASE_URL}data/{cls.FORMAT}/{month}/{station_id.lower()}.txt'

        if not cls.FORMAT:
            raise ValueError(
                'Please provide a format for this historical data requset, or call a formatted child class\'s method.'
            )

        reqs = []
        current_year = datetime.today().year
        last_available_month = (datetime.today() - timedelta(days=31)).month
        has_realtime = (datetime.now() - end_time) < timedelta(days=44)

        for hist_year in range(
            int(start_time.year), min(int(current_year), int(end_time.year) + 1)
        ):
            reqs.append(req_hist_helper_year(hist_year))
        if end_time.year == current_year:
            for hist_month in range(
                int(start_time.month),
                min(int(end_time.month) + 1, int(last_available_month)),
            ):
                reqs.append(req_hist_helper_month(hist_month))
            if int(last_available_month) <= (end_time.month):
                reqs.append(
                    req_hist_helper_month_current(int(last_available_month))
                )

        if has_realtime:
            reqs.append(
                cls._build_request_realtime(station_id=station_id)[
                    0
                ]  # only one URL
            )
        return reqs

    @classmethod
    def _build_request_realtime(cls, station_id: str) -> List[str]:
        if not cls.FILE_FORMAT:
            raise ValueError(
                'Please provide a file format for this historical data requset, or call a formatted child class\'s method.'
            )

        station_id = station_id.upper()
        return [
            f'{cls.BASE_URL}{cls.REAL_TIME_URL_PREFIX}{station_id}{cls.FILE_FORMAT}'
        ]
