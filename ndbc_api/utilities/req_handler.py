"""Handles requests to the NDBC, caching responses for each station.

This module ddefines the `RequestHandler`, a singleton HTTP cache which serves
to handle requests to the NDBC over HTTP and store requests and responses in a
cache.  The cache is segregated by station, such that a cache limit can be
enforced on a station level.

Example:
    TODO

Attributes:
    None.

"""
from typing import Union, List

import requests
from urllib3.util import Retry

from .singleton import Singleton
from .req_cache import RequestCache


class RequestHandler(metaclass=Singleton):
    """The summary line for a class docstring should fit on one line.

    If the class has public attributes, they may be documented here
    in an ``Attributes`` section and follow the same formatting as a
    function's ``Args`` section. Alternatively, attributes may be documented
    inline with the attribute's declaration (see __init__ method below).

    Properties created with the ``@property`` decorator should be documented
    in the property's getter method.

    Attributes:
        stations (:obj:`list`): A list of `Station`s to which requests have
            been made.
        log (:obj:`logging.Logger`): The logger at which to register HTTP
            request and response status codes and headers used for debug
            purposes.
    """

    class Station:

        __slots__ = 'id_', 'reqs'

        def __init__(self, station_id: str, cache_limit: int) -> None:
            self.id_ = station_id
            self.reqs = RequestCache(cache_limit)

    def __init__(
        self,
        cache_limit: int,
        log: 'logging.Logger',
        delay: int,
        retries: int,
        backoff_factor: float,
        headers: dict = None,
        debug: bool = True,
        verify_https: bool = True,
    ) -> None:
        self._cache_limit = cache_limit
        self._request_headers = headers or {}
        self.log = log
        self.stations = []
        self._delay = delay
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._debug = debug
        self._verify_https = verify_https
        self._session = self._create_session()

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
            if s.id_ == station_id:
                return True
        return False

    def get_station(self, station_id: Union[str, int]) -> Station:
        if not self.has_station(station_id):
            self.add_station(station_id=station_id)
        for s in self.stations:
            if s.id_ == station_id:
                return s

    def add_station(self, station_id: Union[str, int]) -> None:
        if isinstance(station_id, int):
            station_id = str(station_id)
        self.stations.append(
            RequestHandler.Station(
                station_id=station_id, cache_limit=self._cache_limit
            )
        )

    def handle_requests(
        self, station_id: Union[str, int], reqs: List[str]
    ) -> List[str]:

        responses = []
        for req in reqs:
            responses.append(
                self.handle_request(station_id=station_id, req=req)
            )
        return responses

    def handle_request(self, station_id: Union[str, int], req: str) -> dict:
        stn = self.get_station(station_id=station_id)
        if req not in stn.reqs.cache:
            resp = self.execute_request(url=req, headers=self._request_headers)
            stn.reqs.put(request=req, response=resp)
        return stn.reqs.get(request=req)

    def execute_request(self, url: str, headers: dict) -> dict:
        response = self._session.get(
            url=url,
            headers=headers,
            allow_redirects=True,
            verify=self._verify_https,
        )
        if self._debug:
            self.log.debug(f'GET: {url}\n\theaders: {headers}')
            self.log.debug(f'response: {response.status_code}.')
        if response.status_code != 200:  # web request did not succeed
            return dict(status=response.status_code, body='')
        return dict(status=response.status_code, body=response.text)

    """ PRIVATE """

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            backoff_factor=self._backoff_factor,
            total=self._retries,
        )
        http_adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount('https://', http_adapter)
        session.mount('http://', http_adapter)
        return session
