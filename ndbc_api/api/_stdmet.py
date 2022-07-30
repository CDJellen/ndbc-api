from datetime import datetime, timedelta
from calendar import month_abbr
from typing import List

from api._base_request import BaseRequst


class Stdmet(BaseRequst):

    REAL_TIME_STDMET_SUFFIX = '.txt'
    HISTORICAL_STDMET_PREFIX = 'data/stdmet/'
    HISTORICAL_STDMET_SUFFIX = 'historical/stdmet/'

    @staticmethod
    def build_request(
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> List[str]:

        is_historical = (datetime.now()-start_time) >= timedelta(days=44)
        if is_historical:
            return Stdmet.build_request_historical(
                station_id,
                start_time,
                end_time
            )
        return Stdmet.build_request_realtime(station_id)

    @staticmethod
    def build_request_historical(
        station_id: str,
        start_time: datetime,
        end_time: datetime
        ) -> str:

        def req_hist_helper_year(req_year: int) -> str:
            return f'{Stdmet.BASE_FILE_URL}{Stdmet.HISTORICAL_URL_PREFIX}{station_id}h{req_year}{Stdmet.HISTORICAL_FILE_EXTENSION_SUFFIX}{Stdmet.HISTORICAL_DATA_PREFIX}{Stdmet.HISTORICAL_STDMET_SUFFIX}'

        def req_hist_helper_month(req_month: int) -> str:
            month = month_abbr[req_month]
            month = month.capitalize()
            return f'{Stdmet.BASE_FILE_URL}{Stdmet.HISTORICAL_URL_PREFIX}{station_id}{req_month}{current_year}{Stdmet.HISTORICAL_FILE_EXTENSION_SUFFIX}{Stdmet.HISTORICAL_DATA_PREFIX}stdmet/{month}/'

        reqs = []
        current_year = datetime.today().year
        has_realtime = (end_time - datetime.now()) < timedelta(days=44)

        for hist_year in range(int(start_time.year), int(current_year)):
            reqs.append(req_hist_helper_year(hist_year))
        for hist_month in range(1, int(end_time.month)):
            reqs.append(req_hist_helper_month(hist_month))
        if has_realtime:
            reqs.append(
                Stdmet.build_request_realtime(
                    station_id=station_id
                )
            )
        return reqs

    @staticmethod
    def build_request_realtime(station_id: str) -> str:
        station_id = station_id.upper()
        return [f'{Stdmet.BASE_FILE_URL}{Stdmet.REAL_TIME_URL_PREFIX}{station_id}{Stdmet.REAL_TIME_STDMET_SUFFIX}']
