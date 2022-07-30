from calendar import month_abbr
from datetime import datetime, timedelta
from typing import List


class BaseRequst():

    BASE_FILE_URL = 'https://www.ndbc.noaa.gov/'
    REAL_TIME_URL_PREFIX = 'data/realtime2/'
    HISTORICAL_FILE_EXTENSION_SUFFIX = '.txt.gz'
    HISTORICAL_DATA_PREFIX = '&dir=data/'
    HISTORICAL_URL_PREFIX = 'view_text_file.php?filename='
    HISTORICAL_SUFFIX = 'historical/'

    @classmethod
    def _build_base_request(
        cls,
        fmt: str,
        file_fmt: str,
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> List[str]:

        is_historical = (datetime.now()-start_time) >= timedelta(days=44)
        if is_historical:
            return BaseRequst._build_request_historical(
                fmt=fmt,
                file_fmt=file_fmt,
                station_id=station_id,
                start_time=start_time,
                end_time=end_time
            )
        return BaseRequst._build_request_realtime(
            file_fmt=file_fmt,
            station_id=station_id
        )

    @staticmethod
    def _build_request_historical(
        fmt: str,
        file_fmt: str,
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> List[str]:

        def req_hist_helper_year(req_year: int) -> str:
            return f'{BaseRequst.BASE_FILE_URL}{BaseRequst.HISTORICAL_URL_PREFIX}{station_id}h{req_year}{BaseRequst.HISTORICAL_FILE_EXTENSION_SUFFIX}{BaseRequst.HISTORICAL_DATA_PREFIX}{BaseRequst.HISTORICAL_SUFFIX}{fmt}/'

        def req_hist_helper_month(req_month: int) -> str:
            month = month_abbr[req_month]
            month = month.capitalize()
            return f'{BaseRequst.BASE_FILE_URL}{BaseRequst.HISTORICAL_URL_PREFIX}{station_id}{req_month}{current_year}{BaseRequst.HISTORICAL_FILE_EXTENSION_SUFFIX}{BaseRequst.HISTORICAL_DATA_PREFIX}{fmt}/{month}/'

        def req_hist_helper_month_current(current_month: int) -> str:
            month = month_abbr[current_month]
            month = month.capitalize()
            return f'{BaseRequst.BASE_FILE_URL}data/{fmt}/{month}/{station_id.lower()}.txt'

        if not fmt:
            raise ValueError('Please provide a format for this historical data requset, or call a formatted child class\'s method.')

        reqs = []
        current_year = datetime.today().year
        last_available_month = (datetime.today()-timedelta(days=44)).month
        has_realtime = (end_time - datetime.now()) < timedelta(days=44)

        for hist_year in range(int(start_time.year), min(int(current_year), int(end_time.year)+1)):
            reqs.append(req_hist_helper_year(hist_year))
        if end_time.year == current_year:
            for hist_month in range(int(start_time.month), min(int(end_time.month)+1, int(last_available_month))):
                reqs.append(req_hist_helper_month(hist_month))
            if int(last_available_month) <= (end_time.month):
                reqs.append(req_hist_helper_month_current(int(last_available_month)))

        if has_realtime:
            reqs.append(
                BaseRequst._build_request_realtime(
                    file_fmt=file_fmt,
                    station_id=station_id
                )[0]  # only one URL
            )
        return reqs

    @staticmethod
    def _build_request_realtime(file_fmt: str, station_id: str) -> List[str]:
        if not file_fmt:
            raise ValueError('Please provide a file format for this historical data requset, or call a formatted child class\'s method.')

        station_id = station_id.upper()
        return [f'{BaseRequst.BASE_FILE_URL}{BaseRequst.REAL_TIME_URL_PREFIX}{station_id}{file_fmt}']
