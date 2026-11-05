"""Handles requests to the NDBC, caching responses for each station.

This module defines the `RequestHandler`, a singleton HTTP cache which serves
to handle requests to the NDBC over HTTP and store requests and responses in a
cache.  The cache is segregated by station, such that a cache limit can be
enforced on a station level.

Example:
    ```python3
        handler = RequestHandler(
            cache_limit=1,
            delay=2,
            retries=3,
            backoff_factor=0.8,
            debug=True,
            verify_https=True,
        )
        response = handler.execute_request(
            url='foo.bar'
        )
    ```

Attributes:
    stations (:obj:`list`): A list of `Station`s to which requests have
        been made.
"""
import logging
import threading
from typing import Callable, List, Optional, Union

import requests
from urllib3.util import Retry

from .req_cache import RequestCache
from .singleton import Singleton


class RequestHandler(metaclass=Singleton):
    """Singleton HTTP request handler with per-station LRU caching.

    Attributes:
        cache_limit (:int:): The handler's global limit for caching
            `NdbcApi` responses. This is implemented as a least-recently
            used cache, designed to conserve NDBC resources when querying
            measurements for a given station over similar time ranges.
        logger (:obj:`logging.Logger`): The logger at which to register HTTP
            request and response status codes and headers used for debug
            purposes.
        delay (:int:): The HTTP(s) request delay parameter, in seconds.
        retries (:int:): The number of times to retry a request to the NDBC data
            service.
        backoff_factor (:float:): The back-off parameter, used in conjunction with
            `retries` to re-attempt requests to the NDBC data service.
        headers (:dict:): The headers with which to execute the requests to the NDBC data
            service.
        debug (:bool:): A flag for verbose logging and response-level status reporting.
            Affects the instance's `logging.Logger` and the behavior of its
            private `RequestHandler` instance.
        verify_https (:bool:): A flag which indicates whether to attempt requests to the
            NDBC data service over HTTP or HTTPS.
    """

    class Station:
        """Per-station request cache container.

        Attributes:
            id_ (:str:): The key for the `Station` object.
            reqs (:obj:`ndbc_api.utilities.RequestCache`): The `RequestCache`
                for the Station with the given `id_`, uses the cache limit of
                its parent `RequestHandler`.
        """
        __slots__ = 'id_', 'reqs'

        def __init__(self, station_id: str, cache_limit: int) -> None:
            self.id_ = station_id
            self.reqs = RequestCache(cache_limit)

    def __init__(
        self,
        cache_limit: int,
        log: Optional[Callable] = None,
        delay: int = 0,
        retries: int = 3,
        backoff_factor: float = 0.5,
        headers: Optional[dict] = None,
        debug: bool = True,
        verify_https: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self._cache_limit = cache_limit
        self._request_headers = headers or {}
        self.log = log or (lambda *args, **kwargs: None)
        self.stations = []
        self._delay = delay
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._debug = debug
        self._verify_https = verify_https
        self._session = self._create_session()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        if getattr(self, '_session', None) is not None:
            self._session.close()

    def get_cache_limit(self) -> int:
        """Return the current station-level cache limit for NDBC requests."""
        return self._cache_limit

    def set_cache_limit(self, cache_limit: int) -> None:
        """Set a new station-level cache limit for NDBC requests."""
        with self._lock:
            self._cache_limit = cache_limit
            for s in self.stations:
                s.reqs.set_capacity(cache_limit)

    def get_headers(self) -> dict:
        """Return the current headers used for NDBC data service requests."""
        return self._request_headers

    def update_headers(self, new: dict) -> None:
        """Add new headers to future NDBC data service requests."""
        with self._lock:
            self._request_headers.update(new)

    def set_headers(self, request_headers: dict) -> None:
        """Reset the request headers using the new supplied headers."""
        with self._lock:
            self._request_headers = request_headers

    def has_station(self, station_id: Union[str, int]) -> bool:
        """Determine if the NDBC API already made a request to this station."""
        with self._lock:
            for s in self.stations:
                if s.id_ == station_id:
                    return True
            return False

    def get_station(self, station_id: Union[str, int]) -> Station:
        """Get `RequestCache` with `id_` matching the supplied `station_id`."""
        if isinstance(station_id, int):
            station_id = str(station_id)
        with self._lock:
            if not self.has_station(station_id):
                self.log(logging.DEBUG,
                         station_id=station_id,
                         message=f'Adding station {station_id} to cache.')
                self.add_station(station_id=station_id)
            for s in self.stations:
                if s.id_ == station_id:
                    self.log(logging.DEBUG,
                             station_id=station_id,
                             message=f'Found station {station_id} in cache.')
                    return s

    def add_station(self, station_id: Union[str, int]) -> None:
        """Add new `RequestCache` for the supplied `station_id`."""
        with self._lock:
            self.stations.append(
                RequestHandler.Station(station_id=station_id,
                                       cache_limit=self._cache_limit))

    def handle_requests(self, station_id: Union[str, int],
                        reqs: List[str]) -> List[dict]:  # pragma: no cover
        """Handle many string-valued requests against a supplied station."""
        responses = []
        self.log(
            logging.INFO,
            message=f'Handling {len(reqs)} requests for station {station_id}.')
        for req in reqs:
            responses.append(self.handle_request(station_id=station_id,
                                                 req=req))
        return responses

    def handle_request(self, station_id: Union[str, int], req: str) -> dict:
        """Handle a string-valued request against a supplied station."""
        with self._lock:
            stn = self.get_station(station_id=station_id)
            self.log(logging.DEBUG, message=f'Handling request {req}.')
            if req in stn.reqs.cache:
                self.log(logging.DEBUG, message=f'Request {req} already in cache.')
                return stn.reqs.get(request=req)

        self.log(logging.DEBUG, message=f'Adding request {req} to cache.')
        resp = self.execute_request(url=req,
                                    station_id=station_id,
                                    headers=self._request_headers)
        with self._lock:
            stn.reqs.put(request=req, response=resp)
            return stn.reqs.get(request=req)

    def execute_request(self, station_id: Union[str, int], url: str,
                        headers: dict) -> dict:  # pragma: no cover
        """Execute a request with the current headers to NDBC data service."""
        self.log(logging.DEBUG,
                 station_id=station_id,
                 message=f'GET: {url}',
                 extra_data={'headers': headers})
        response = self._session.get(
            url=url,
            headers=headers,
            allow_redirects=True,
            verify=self._verify_https,
        )
        self.log(logging.DEBUG,
                 station_id=station_id,
                 message=f'Response status: {response.status_code}')
        if response.status_code != 200:  # web request did not succeed
            return dict(status=response.status_code, body='')
        content_type = (response.headers.get('Content-Type') or '').lower()
        if any([
                'netcdf' in content_type,
                'octet' in content_type,
        ]):
            return dict(status=response.status_code, body=response.content)
        return dict(status=response.status_code, body=response.text)

    """ PRIVATE """

    def _create_session(self) -> requests.Session:
        """create a new `Session` using `RequestHandler` configuration."""
        self.log(logging.DEBUG, message='Creating new session.')
        session = requests.Session()
        retry = Retry(
            backoff_factor=self._backoff_factor,
            total=self._retries,
        )
        http_adapter = requests.adapters.HTTPAdapter(max_retries=retry)
        session.mount('https://', http_adapter)
        session.mount('http://', http_adapter)
        self.log(logging.INFO, message='Created session.')
        return session
