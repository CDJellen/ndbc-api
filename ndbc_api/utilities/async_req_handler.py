"""Async request handler for the NDBC API.

Mirrors :class:`RequestHandler` using ``aiohttp.ClientSession`` for
non-blocking I/O.  Shares the same :class:`RequestCache` and
:class:`Station` patterns as its synchronous counterpart.

Example::

    async with AsyncRequestHandler(cache_limit=36, ...) as handler:
        resp = await handler.execute_request(
            station_id='tplm2', url='https://...', headers={}
        )
"""
import asyncio
import logging
from typing import Dict, List, Union, Callable

import aiohttp

from .req_cache import RequestCache


class AsyncRequestHandler:
    """Async counterpart to :class:`RequestHandler`.

    Uses ``aiohttp.ClientSession`` for non-blocking HTTP and reuses the
    synchronous :class:`RequestCache` for per-station LRU caching.

    This class is **not** a singleton — callers manage its lifecycle
    explicitly via ``async with``.

    Attributes:
        stations: A list of cached ``Station`` objects.
    """

    class Station:
        """Per-station request cache container."""
        __slots__ = 'id_', 'reqs'

        def __init__(self, station_id: str, cache_limit: int) -> None:
            self.id_ = station_id
            self.reqs = RequestCache(cache_limit)

    def __init__(
        self,
        cache_limit: int,
        log: Callable,
        delay: int,
        retries: int,
        backoff_factor: float,
        headers: dict = None,
        debug: bool = True,
        verify_https: bool = True,
        max_connections: int = 10,
    ) -> None:
        self._cache_limit = cache_limit
        self._request_headers = headers or {}
        self.log = log
        self.stations: list = []
        self._delay = delay
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._debug = debug
        self._verify_https = verify_https
        self._max_connections = max_connections
        self._semaphore = asyncio.Semaphore(max_connections)
        self._station_locks: Dict[str, asyncio.Lock] = {}
        self._session: aiohttp.ClientSession = None

    # --- context manager ---------------------------------------------------

    async def __aenter__(self) -> 'AsyncRequestHandler':
        connector = aiohttp.TCPConnector(
            ssl=self._verify_https,
            limit=self._max_connections,
        )
        self._session = aiohttp.ClientSession(connector=connector)
        self.log(logging.DEBUG, message='Created async session.')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self.log(logging.DEBUG, message='Closed async session.')

    # --- public cache helpers ----------------------------------------------

    def get_cache_limit(self) -> int:
        """Return the current station-level cache limit."""
        return self._cache_limit

    def set_cache_limit(self, cache_limit: int) -> None:
        """Set a new station-level cache limit."""
        self._cache_limit = cache_limit

    def get_headers(self) -> dict:
        """Return the current request headers."""
        return self._request_headers

    def update_headers(self, new: dict) -> None:
        """Merge new headers into the existing set."""
        self._request_headers.update(new)

    def set_headers(self, request_headers: dict) -> None:
        """Replace all request headers."""
        self._request_headers = request_headers

    def has_station(self, station_id: Union[str, int]) -> bool:
        """Check if we have a cache for this station."""
        for s in self.stations:
            if s.id_ == station_id:
                return True
        return False

    def get_station(self, station_id: Union[str, int]) -> 'Station':
        """Get or create a :class:`Station` cache entry."""
        if isinstance(station_id, int):
            station_id = str(station_id)
        if not self.has_station(station_id):
            self.log(logging.DEBUG,
                     station_id=station_id,
                     message=f'Adding station {station_id} to cache.')
            self.add_station(station_id=station_id)
        for s in self.stations:
            if s.id_ == station_id:
                return s

    def add_station(self, station_id: Union[str, int]) -> None:
        """Create a new :class:`Station` cache entry."""
        self.stations.append(
            AsyncRequestHandler.Station(station_id=station_id,
                                        cache_limit=self._cache_limit))

    # --- async I/O ---------------------------------------------------------

    async def handle_requests(self, station_id: Union[str, int],
                              reqs: List[str]) -> List[dict]:
        """Handle a batch of requests for one station, sequentially.

        Requests within a single station are processed one at a time
        (mirroring the synchronous handler).  Concurrency *across*
        stations is achieved at the ``AsyncNdbcApi`` level via
        ``asyncio.gather``.
        """
        self.log(
            logging.INFO,
            message=f'Handling {len(reqs)} requests for station {station_id}.')
        responses = []
        for req in reqs:
            responses.append(
                await self.handle_request(station_id=station_id, req=req))
        return responses

    async def handle_request(self, station_id: Union[str, int],
                             req: str) -> dict:
        """Check cache, fetch on miss, return response dict.

        Uses a per-station ``asyncio.Lock`` so that concurrent
        coroutines requesting the same URL don't both fire the HTTP
        request.
        """
        if isinstance(station_id, int):
            station_id = str(station_id)
        if station_id not in self._station_locks:
            self._station_locks[station_id] = asyncio.Lock()

        async with self._station_locks[station_id]:
            stn = self.get_station(station_id=station_id)
            self.log(logging.DEBUG, message=f'Handling request {req}.')
            if req not in stn.reqs.cache:
                self.log(logging.DEBUG,
                         message=f'Adding request {req} to cache.')
                resp = await self.execute_request(
                    url=req,
                    station_id=station_id,
                    headers=self._request_headers)
                stn.reqs.put(request=req, response=resp)
            else:
                self.log(logging.DEBUG,
                         message=f'Request {req} already in cache.')
        return stn.reqs.get(request=req)

    async def execute_request(self, station_id: Union[str, int], url: str,
                              headers: dict) -> dict:
        """Execute an HTTP GET with retry, exponential backoff, and throttle.

        Acquires the concurrency semaphore before making the request
        so that at most ``max_connections`` requests are in-flight
        globally.  Also sleeps ``delay`` ms before each request
        to throttle request rate (mirroring the sync handler).
        """
        self.log(logging.DEBUG,
                 station_id=station_id,
                 message=f'GET: {url}',
                 extra_data={'headers': headers})

        last_exc = None
        for attempt in range(self._retries + 1):
            async with self._semaphore:
                # Throttle: sleep before issuing the request
                if self._delay > 0:
                    await asyncio.sleep(self._delay / 1000)
                try:
                    async with self._session.get(
                        url,
                        headers=headers,
                        allow_redirects=True,
                    ) as response:
                        status = response.status
                        self.log(logging.DEBUG,
                                 station_id=station_id,
                                 message=f'Response status: {status}')
                        if status != 200:
                            return dict(status=status, body='')
                        content_type = response.headers.get(
                            'Content-Type', '').lower()
                        if any(t in content_type
                               for t in ('netcdf', 'octet')):
                            body = await response.read()
                        else:
                            body = await response.text()
                        return dict(status=status, body=body)
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    last_exc = exc
                    if attempt < self._retries:
                        wait = self._backoff_factor * (2 ** attempt)
                        self.log(logging.DEBUG,
                                 station_id=station_id,
                                 message=(f'Retry {attempt + 1}/{self._retries} '
                                          f'after {wait:.1f}s'))
                        await asyncio.sleep(wait)

        self.log(logging.WARNING,
                 station_id=station_id,
                 message=f'All {self._retries} retries exhausted for {url}')
        return dict(status=0, body='')
