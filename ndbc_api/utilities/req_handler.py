from typing import Union, List

import requests

from .singleton import Singleton
from .req_cache import RequestCache


class RequestHandler(metaclass=Singleton):

    class Station:

        __slots__ = 'id', 'reqs'

        def __init__(self, station_id: str, cache_limit: int) -> None:
            self.id = station_id
            self.reqs = RequestCache(cache_limit)

    def __init__(self, cache_limit, log) -> None:
        self._cache_limit = cache_limit
        self._request_headers = {}
        self.log = log
        self.stations = []

    def get_cache_limit(self) -> int:
        return self._cache_limit

    def set_cache_limit(self, cache_limit: int) -> None:
        self._cache_limit = cache_limit

    def update_headers(self, new: dict) -> None:
        self._request_headers.update(new)

    def set_headers(self, request_headers: dict) -> None:
        self._request_headers = request_headers

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
            resp = self.execute_request(url=req, headers=self._request_headers)
            stn.reqs.put(request=req, reponse=resp)
        return stn.reqs.get(request=req)

    def execute_request(self, url: str, headers: dict) -> dict:
        self.log.debug(f'GET: {url}\n\theaders: {headers}')
        response = requests.get(url=url)
        self.log.debug(f'response: {response.status_code}.')
        if response.status_code != 200: # web request did not succeed
            return dict(status=response.status_code, body='')
        return dict(status=response.status_code, body=response.text)
