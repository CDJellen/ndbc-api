from datetime import datetime, timedelta
import os
from typing import List

from ndbc_api.api.requests.opendap._base import BaseRequest


class HfradarRequest(BaseRequest):

    # example url: https://dods.ndbc.noaa.gov/thredds/fileServer/hfradar/202505301600_hfr_uswc_1km_rtv_uwls_NDBC.nc
    BASE_URL = 'https://dods.ndbc.noaa.gov/thredds/'
    URL_PREFIX = 'fileServer/hfradar/'
    URL_SUFFIX = '_rtv_uwls_NDBC'
    FORMAT = 'nc'
    
    @classmethod
    def build_request(cls, station_id: str, start_time: datetime,
                      end_time: datetime) -> List[str]:
        """Builds the request for HFRadar data."""

        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise ValueError('start_time and end_time must be datetime objects.')

        if 'MOCKDATE' in os.environ:
            start_time = datetime.strptime(os.getenv('MOCKDATE'), '%Y-%m-%d')
            end_time = start_time + timedelta(days=1)

        # Each file is stored at a 1 hour interval, so we need to round the start and end time
        start_time = start_time.replace(minute=0, second=0, microsecond=0)
        end_time = end_time.replace(minute=0, second=0, microsecond=0)
        
        reqs = []
        for hour in range(int((end_time - start_time).total_seconds() // 3600) + 1):
            current_time = start_time + timedelta(hours=hour)
            year = current_time.year
            month = f'{current_time.month:02d}'
            day = f'{current_time.day:02d}'
            hour_str = f'{current_time.hour:02d}'
            req_url = f'{cls.BASE_URL}{cls.URL_PREFIX}{year}{month}{day}{hour_str}00_hfr_{station_id}{cls.URL_SUFFIX}.{cls.FORMAT}'
            reqs.append(req_url)
        
        return reqs
