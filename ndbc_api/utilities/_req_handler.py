from datetime import datetime, timedelta
from calendar import month_abbr
from typing import Union, List

import requests

from .singleton import Singleton
from .req_cache import RequestCache


class RequestHandler(metaclass=Singleton):

    BASE_FILE_URL = 'https://www.ndbc.noaa.gov/'
    REAL_TIME_URL_PREFIX = 'data/realtime2/'
    REAL_TIME_STDMET_SUFFIX = '.txt'
    REAL_TIME_CWIND_SUFFIX = '.cwind'
    HISTORICAL_FILE_EXTENSION_SUFFIX = '.txt.gz'
    HISTORICAL_DATA_PREFIX = '&dir=data/'
    HISTORICAL_URL_PREFIX = 'view_text_file.php?filename='
    HISTORICAL_STDMET_PREFIX = 'data/stdmet/'
    HISTORICAL_STDMET_SUFFIX = 'historical/stdmet/'
    HISTORICAL_CWIND_PREFIX = 'data/cwind/'
    HISTORICAL_CWIND_SUFFIX = 'historical/cwind/'

    class Station:
        __slots__ = 'id', 'reqs'

        def __init__(self, station_id: str, cache_limit: int) -> None:
            self.id = station_id
            self.reqs = RequestCache(cache_limit)

    def __init__(self, cache_limit, log) -> None:
        self._cache_limit = cache_limit
        self.log = log
        self.stations = []
        self._modes = self._register_modes()

    def update_cache_limit(self, cache_limit: int) -> None:
        self._cache_limit = cache_limit

    def has_station(self, station_id: Union[str, int]) -> bool:
        for s in self.stations:
            if s.id == station_id:
                return True
        return False

    def get_station(self, station_id: Union[str, int]) -> Station:
        if not self.has_station(station_id):
            self.add_station(station_id=station_id)
        for s in self.stations:
            if s.id == station_id:
                return s

    def add_station(self, station_id: Union[str, int]) -> None:
        if isinstance(station_id, int):
            station_id = str(station_id)
        self.stations.append(
            RequestHandler.Station(
                station_id=station_id,
                cache_limit=self._cache_limit)
            )

    def build_request(
        self,
        station_id: Union[str, int],
        mode: str,
        start_time: datetime,
        end_time: datetime
        ) -> Union[str, List[str]]:

        if start_time > end_time:
            raise ValueError
        if mode in self._modes:
            return self._modes[mode](station_id, start_time, end_time)
        else:
            raise NotImplementedError

    def build_request_stdmet(
        self,
        station_id: Union[str, int],
        start_time: datetime,
        end_time: datetime
        ) -> str:

        is_historical = (datetime.now()-start_time) >= timedelta(days=44)
        requested_station = str(station_id)
        if is_historical:
            return self.build_request_stdmet_historical(
                requested_station,
                start_time,
                end_time
            )
        return self.build_request_stdmet_realtime(requested_station)

    def build_request_stdmet_historical(
        self,
        requested_station: str,
        start_time: datetime,
        end_time: datetime
        ) -> str:

        def req_hist_helper_year(req_year: int) -> str:
            return f'{RequestHandler.BASE_FILE_URL}{RequestHandler.HISTORICAL_URL_PREFIX}{requested_station}h{req_year}{RequestHandler.HISTORICAL_FILE_EXTENSION_SUFFIX}{RequestHandler.HISTORICAL_DATA_PREFIX}{RequestHandler.HISTORICAL_STDMET_SUFFIX}'

        def req_hist_helper_month(req_month: int) -> str:
            month = month_abbr[req_month]
            month = month.capitalize()
            return f'{RequestHandler.BASE_FILE_URL}{RequestHandler.HISTORICAL_URL_PREFIX}{requested_station}{req_month}{current_year}{RequestHandler.HISTORICAL_FILE_EXTENSION_SUFFIX}{RequestHandler.HISTORICAL_DATA_PREFIX}stdmet/{month}/'

        reqs = []
        current_year = datetime.today().year
        has_realtime = (end_time - datetime.now()) < timedelta(days=44)

        for hist_year in range(int(start_time.year), int(current_year)):
            reqs.append(req_hist_helper_year(hist_year))
        for hist_month in range(1, int(end_time.month)):
            reqs.append(req_hist_helper_month(hist_month))
        if has_realtime:
            reqs.append(
                self.build_request_stdmet_realtime(
                    requested_station=requested_station
                )
            )

        return reqs

    def build_request_stdmet_realtime(self, requested_station: str) -> str:
        requested_station = requested_station.upper()
        return f'{RequestHandler.BASE_FILE_URL}{RequestHandler.REAL_TIME_URL_PREFIX}{requested_station}{RequestHandler.REAL_TIME_STDMET_SUFFIX}'

    def build_request_cwind(
        self,
        station_id: Union[str, int],
        start_time: datetime,
        end_time: datetime
        ) -> str:

        is_historical = (datetime.now()-start_time) >= timedelta(days=44)
        requested_station = str(station_id)
        if is_historical:
            return self.build_request_cwind_historical(
                requested_station,
                start_time,
                end_time
            )
        return self.build_request_cwind_realtime(requested_station)

    def build_request_cwind_historical(
        self,
        requested_station: str,
        start_time: datetime,
        end_time: datetime
        ) -> str:

        def req_hist_helper_year(req_year: int) -> str:
            return f'{RequestHandler.BASE_FILE_URL}{RequestHandler.HISTORICAL_URL_PREFIX}{requested_station}c{req_year}{RequestHandler.HISTORICAL_FILE_EXTENSION_SUFFIX}{RequestHandler.HISTORICAL_DATA_PREFIX}{RequestHandler.HISTORICAL_CWIND_SUFFIX}'

        def req_hist_helper_month(req_month: int) -> str:
            month = month_abbr[req_month]
            month = month.capitalize()
            return f'{RequestHandler.BASE_FILE_URL}{RequestHandler.HISTORICAL_URL_PREFIX}{requested_station}{req_month}{current_year}{RequestHandler.HISTORICAL_FILE_EXTENSION_SUFFIX}{RequestHandler.HISTORICAL_DATA_PREFIX}cwind/{month}/'

        reqs = []
        current_year = datetime.today().year
        has_realtime = (datetime.now() - end_time) < timedelta(days=44)

        for hist_year in range(int(start_time.year), int(current_year)):
            reqs.append(req_hist_helper_year(hist_year))
        for hist_month in range(1, int(end_time.month)):
            reqs.append(req_hist_helper_month(hist_month))
        if has_realtime:
            reqs.append(
                self.build_request_stdmet_realtime(
                    requested_station=requested_station
                )
            )

        return reqs

    def build_request_cwind_realtime(self, requested_station: str) -> str:
        requested_station = requested_station.upper()
        return f'{RequestHandler.BASE_FILE_URL}{RequestHandler.REAL_TIME_URL_PREFIX}{requested_station}{RequestHandler.REAL_TIME_CWIND_SUFFIX}'

    def handle_requests(
        self,
        station_id: Union[str, int],
        reqs: List[str]
        ) -> List[str]:

        responses = []
        for req in reqs:
            responses.append(
                self.handle_request(
                    station_id=station_id,
                    req=req
                )
            )
        return responses

    def handle_request(self, station_id: Union[str, int], req: str) -> dict:
        stn = self.get_station(station_id=station_id)
        if req not in stn.reqs.cache:
            resp = self.execute_request(url=req)
            stn.reqs.put(request=req, reponse=resp)
        return stn.reqs.get(request=req)

    def execute_request(self, url: str, headers: dict) -> dict:
        self.log.debug(f'GET: {url}\n\theaders: {headers}')
        response = requests.get(url=url)
        print(f'RESPONSE: {response.status_code}')
        if response.status_code != 200: # web request did not succeed
            return dict(status=response.status_code, body='')
        return dict(status=response.status_code, body=response.text)

    def get_station_data(self, station_id: Union[str, int], mode: str, start_time: datetime, end_time: datetime) -> Union[str, List[str]]:
        reqs = self.build_request(station_id=station_id, mode=mode, start_time=start_time, end_time=end_time)
        if isinstance(reqs, list):
            responses = self.handle_requests(station_id=station_id, reqs=reqs)
            return responses
        else:  # only one request requried to capture NDBC data
            response = self.handle_request(station_id=station_id, req=reqs)
            return response

    def _register_modes(self):
        return {
            'stdmet': self.get_stdmet,
            'cwind': self.get_cwind,
            'ocean': self.get_ocean,
            'adcp': self.get_adcp,
            'data_spec': self.get_data_spec,
            'dmv': self.get_dmv,
            'supl': self.get_supl,
            'swdir': self.get_swdir,
            'swdir2': self.get_swdir2,
            'swr1': self.get_swr1,
            'swr2': self.get_swr2,
        }