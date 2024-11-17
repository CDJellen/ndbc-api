import os
from datetime import datetime, timedelta
from typing import List

from ndbc_api.api.requests.opendap._core import CoreRequest


class BaseRequest(CoreRequest):

    # example url: https://dods.ndbc.noaa.gov/thredds/fileServer/data/adcp/41001/41001a2010.nc
    # example url: https://dods.ndbc.noaa.gov/thredds/fileServer/data/stdmet/tplm2/tplm2h2021.nc
    URL_PREFIX = 'fileServer/data/'
    FORMAT = ''
    HISTORICAL_IDENTIFIER = ''  # we keep the same structure as the http requests
    FILE_FORMAT = 'nc'

    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:

        if 'MOCKDATE' in os.environ:
            now = datetime.strptime(os.getenv('MOCKDATE'), '%Y-%m-%d')
        else:
            now = datetime.now()
        is_historical = (now - start_time) >= timedelta(
            days=45)  # we use 45 rather than 44 for opendap data
        if is_historical:
            return cls._build_request_historical(
                station_id=station_id,
                start_time=start_time,
                end_time=end_time,
                now=now,
            )
        return cls._build_request_realtime(station_id=station_id)

    @classmethod
    def _build_request_historical(
        cls,
        station_id: str,
        start_time: datetime,
        end_time: datetime,
        now: datetime,
    ) -> List[str]:

        def req_hist_helper_year(req_year: int) -> str:
            return f'{cls.BASE_URL}{cls.URL_PREFIX}{cls.FORMAT}/{station_id.lower()}/{station_id.lower()}{cls.HISTORICAL_IDENTIFIER}{req_year}.{cls.FILE_FORMAT}'

        if not cls.FORMAT:  # pragma: no cover
            raise ValueError(
                'Please provide a format for this historical data request, or call a formatted child class\'s method.'
            )
        # store request urls
        reqs = []

        current_year = now.year
        has_realtime = (now - end_time) <= timedelta(days=45)

        # handle year requests
        for hist_year in range(int(start_time.year),
                               min(int(current_year),
                                   int(end_time.year) + 1)):
            reqs.append(req_hist_helper_year(hist_year))

        if has_realtime:
            reqs.append(
                cls._build_request_realtime(
                    station_id=station_id)[0]  # only one URL
            )
        return reqs

    @classmethod
    def _build_request_realtime(cls, station_id: str) -> List[str]:
        if not cls.FILE_FORMAT:
            raise ValueError(
                'Please provide a file format for this historical data request, or call a formatted child class\'s method.'
            )

        station_id = station_id.upper()
        # realtime data uses 9999 as the year part
        return [
            f'{cls.BASE_URL}{cls.URL_PREFIX}{cls.FORMAT}/{station_id.lower()}/{station_id.lower()}{cls.HISTORICAL_IDENTIFIER}9999.{cls.FILE_FORMAT}'
        ]
