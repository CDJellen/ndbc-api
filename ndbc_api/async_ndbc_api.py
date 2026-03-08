"""An async API for retrieving data from the NDBC.

This module defines the ``AsyncNdbcApi``, which mirrors the public API
of :class:`NdbcApi` with ``async def`` methods backed by ``aiohttp``
for non-blocking I/O and ``asyncio.gather`` for concurrent
multi-station queries.

Requires the ``async`` extra::

    pip install ndbc-api[async]

Example::

    from ndbc_api import AsyncNdbcApi

    async with AsyncNdbcApi() as api:
        available_stations = await api.stations()
        modes = api.get_modes()
        df_stdmet_tplm2 = await api.get_data(
            'tplm2',
            'stdmet',
            '2020-01-01',
            '2022-01-01',
            as_df=True,
        )

Attributes:
    log (:obj:`logging.Logger`): The logger at which to register HTTP
        request and response status codes and headers used for debug
        purposes.
    headers (dict): The request headers for use in the async API's
        request handler.
"""
import asyncio
import logging
import pickle
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import xarray

from .config import (
    DEFAULT_CACHE_LIMIT,
    HTTP_BACKOFF_FACTOR,
    HTTP_DEBUG,
    HTTP_DELAY,
    HTTP_RETRY,
    LOGGER_NAME,
    VERIFY_HTTPS,
)
from .exceptions import (
    HandlerException,
    ParserException,
    RequestException,
    ResponseException,
    TimestampException,
)
from .utilities.async_req_handler import AsyncRequestHandler
from .utilities.log_formatter import LogFormatter
from .utilities.data_helpers import (
    parse_station_id,
    handle_timestamp,
    enforce_timerange,
    handle_data,
    handle_accumulate_data,
)
from .utilities.opendap.dataset import (
    concat_datasets,
    filter_dataset_by_time_range,
    filter_dataset_by_variable,
    merge_datasets,
)

# --- HTTP request builders ---------------------------------------------------
from .api.requests.http.adcp import AdcpRequest as HttpAdcpRequest
from .api.requests.http.cwind import CwindRequest as HttpCwindRequest
from .api.requests.http.ocean import OceanRequest as HttpOceanRequest
from .api.requests.http.spec import SpecRequest as HttpSpecRequest
from .api.requests.http.stdmet import StdmetRequest as HttpStdmetRequest
from .api.requests.http.supl import SuplRequest as HttpSuplRequest
from .api.requests.http.swden import SwdenRequest as HttpSwdenRequest
from .api.requests.http.swdir import SwdirRequest as HttpSwdirRequest
from .api.requests.http.swdir2 import Swdir2Request as HttpSwdir2Request
from .api.requests.http.swr1 import Swr1Request as HttpSwr1Request
from .api.requests.http.swr2 import Swr2Request as HttpSwr2Request

# --- HTTP parsers ------------------------------------------------------------
from .api.parsers.http.adcp import AdcpParser as HttpAdcpParser
from .api.parsers.http.cwind import CwindParser as HttpCwindParser
from .api.parsers.http.ocean import OceanParser as HttpOceanParser
from .api.parsers.http.spec import SpecParser as HttpSpecParser
from .api.parsers.http.stdmet import StdmetParser as HttpStdmetParser
from .api.parsers.http.supl import SuplParser as HttpSuplParser
from .api.parsers.http.swden import SwdenParser as HttpSwdenParser
from .api.parsers.http.swdir import SwdirParser as HttpSwdirParser
from .api.parsers.http.swdir2 import Swdir2Parser as HttpSwdir2Parser
from .api.parsers.http.swr1 import Swr1Parser as HttpSwr1Parser
from .api.parsers.http.swr2 import Swr2Parser as HttpSwr2Parser

# --- OpenDAP request builders ------------------------------------------------
from .api.requests.opendap.adcp import AdcpRequest as OpendapAdcpRequest
from .api.requests.opendap.cwind import CwindRequest as OpendapCwindRequest
from .api.requests.opendap.ocean import OceanRequest as OpendapOceanRequest
from .api.requests.opendap.pwind import PwindRequest as OpendapPwindRequest
from .api.requests.opendap.stdmet import StdmetRequest as OpendapStdmetRequest
from .api.requests.opendap.swden import SwdenRequest as OpendapSwdenRequest
from .api.requests.opendap.wlevel import WlevelRequest as OpendapWlevelRequest
from .api.requests.opendap.hfradar import HfradarRequest as OpendapHfradarRequest

# --- OpenDAP parsers ---------------------------------------------------------
from .api.parsers.opendap.adcp import AdcpParser as OpendapAdcpParser
from .api.parsers.opendap.cwind import CwindParser as OpendapCwindParser
from .api.parsers.opendap.ocean import OceanParser as OpendapOceanParser
from .api.parsers.opendap.pwind import PwindParser as OpendapPwindParser
from .api.parsers.opendap.stdmet import StdmetParser as OpendapStdmetParser
from .api.parsers.opendap.swden import SwdenParser as OpendapSwdenParser
from .api.parsers.opendap.wlevel import WlevelParser as OpendapWlevelParser
from .api.parsers.opendap.hfradar import HfradarParser as OpendapHfradarParser

# --- station request builders & parsers --------------------------------------
from .api.requests.http.active_stations import ActiveStationsRequest
from .api.requests.http.historical_stations import HistoricalStationsRequest
from .api.requests.http.station_metadata import MetadataRequest
from .api.requests.http.station_realtime import RealtimeRequest
from .api.requests.http.station_historical import HistoricalRequest
from .api.parsers.http.active_stations import ActiveStationsParser
from .api.parsers.http.historical_stations import HistoricalStationsParser
from .api.parsers.http.station_metadata import MetadataParser
from .api.parsers.http.station_realtime import RealtimeParser
from .api.parsers.http.station_historical import HistoricalParser

# --- station handler for geo helpers -----------------------------------------
from .api.handlers.http.stations import StationsHandler


class AsyncNdbcApi:
    """Async API for retrieving data from the NDBC.

    Provides the same public API surface as :class:`NdbcApi`, but with
    ``async def`` methods backed by ``aiohttp`` for non-blocking I/O
    and ``asyncio.gather`` for concurrent multi-station queries.

    Unlike ``NdbcApi``, this class is **not** a singleton.  Callers
    manage its lifecycle via ``async with``::

        async with AsyncNdbcApi() as api:
            df = await api.stations()

    Requires the ``async`` extra::

        pip install ndbc-api[async]

    Attributes:
        logger (:obj:`logging.Logger`): The logger at which to register
            HTTP request and response status codes.
        headers (dict): The request headers for use in the API's async
            request handler.
        cache_limit (int): The handler's per-station LRU cache limit.
    """

    # -------------------------------------------------------------------
    # Dispatch tables: mode -> (RequestBuilder, Parser)
    # -------------------------------------------------------------------
    _HTTP_DISPATCH = {
        'adcp': (HttpAdcpRequest, HttpAdcpParser),
        'cwind': (HttpCwindRequest, HttpCwindParser),
        'ocean': (HttpOceanRequest, HttpOceanParser),
        'spec': (HttpSpecRequest, HttpSpecParser),
        'stdmet': (HttpStdmetRequest, HttpStdmetParser),
        'supl': (HttpSuplRequest, HttpSuplParser),
        'swden': (HttpSwdenRequest, HttpSwdenParser),
        'swdir': (HttpSwdirRequest, HttpSwdirParser),
        'swdir2': (HttpSwdir2Request, HttpSwdir2Parser),
        'swr1': (HttpSwr1Request, HttpSwr1Parser),
        'swr2': (HttpSwr2Request, HttpSwr2Parser),
    }

    _OPENDAP_DISPATCH = {
        'adcp': (OpendapAdcpRequest, OpendapAdcpParser),
        'cwind': (OpendapCwindRequest, OpendapCwindParser),
        'ocean': (OpendapOceanRequest, OpendapOceanParser),
        'pwind': (OpendapPwindRequest, OpendapPwindParser),
        'stdmet': (OpendapStdmetRequest, OpendapStdmetParser),
        'swden': (OpendapSwdenRequest, OpendapSwdenParser),
        'wlevel': (OpendapWlevelRequest, OpendapWlevelParser),
        'hfradar': (OpendapHfradarRequest, OpendapHfradarParser),
    }

    logger = logging.getLogger(LOGGER_NAME)

    def __init__(
        self,
        logging_level: int = logging.WARNING if HTTP_DEBUG else logging.ERROR,
        filename: Any = None,
        cache_limit: int = DEFAULT_CACHE_LIMIT,
        headers: Optional[dict] = None,
        delay: int = HTTP_DELAY,
        retries: int = HTTP_RETRY,
        backoff_factor: float = HTTP_BACKOFF_FACTOR,
        verify_https: bool = VERIFY_HTTPS,
        debug: bool = HTTP_DEBUG,
    ):
        """Initialise the ``AsyncNdbcApi`` and configure logging.

        Args:
            logging_level: The ``logging.Logger``'s log level.
            filename: If provided, logs are written to this filepath
                instead of ``stderr``.
            cache_limit: The per-station LRU cache limit for the
                underlying :class:`AsyncRequestHandler`.
            headers: Optional HTTP headers to send with each request.
            delay: The inter-request delay in milliseconds.
            retries: The maximum number of retry attempts per request.
            backoff_factor: The exponential back-off factor applied
                between retries.
            verify_https: Whether to verify TLS certificates.
            debug: If ``True``, enables verbose logging.
        """
        self.cache_limit = cache_limit
        self.headers = headers or {}
        self._delay = delay
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._verify_https = verify_https
        self._debug = debug
        self._handler: AsyncRequestHandler = None
        self.configure_logging(level=logging_level, filename=filename)

    # --- context manager ---------------------------------------------------

    async def __aenter__(self) -> 'AsyncNdbcApi':
        self._handler = AsyncRequestHandler(
            cache_limit=self.cache_limit,
            log=self.log,
            delay=self._delay,
            retries=self._retries,
            backoff_factor=self._backoff_factor,
            headers=self.headers,
            debug=self._debug,
            verify_https=self._verify_https,
        )
        await self._handler.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._handler:
            await self._handler.__aexit__(exc_type, exc_val, exc_tb)

    # --- logging -----------------------------------------------------------

    def configure_logging(self, level=logging.WARNING, filename=None):
        """Configure logging for the ``AsyncNdbcApi``.

        Args:
            level: The logging level (e.g. ``logging.WARNING``).
            filename: If provided, logs are written to this filepath.
                Otherwise a ``StreamHandler`` is used.
        """
        self.logger.setLevel(level)
        self.logger.handlers = []
        formatter = LogFormatter()
        if filename:
            fh = logging.FileHandler(filename)
            fh.setLevel(level)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)
        else:
            ch = logging.StreamHandler()
            ch.setLevel(level)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def log(
        self,
        level: int,
        station_id: Union[int, str, None] = None,
        mode: Union[str, None] = None,
        message: Union[str, None] = None,
        **extra_data,
    ):
        """Log a structured message with metadata.

        Args:
            level: The logging level.
            station_id: The NDBC station ID, if applicable.
            mode: The data mode, if applicable.
            message: The log message.
            **extra_data: Additional key-value pairs to include in the
                structured log entry.
        """
        log_data = {
            k: v
            for k, v in {
                'station_id': station_id,
                'mode': mode,
                'message': message,
                **extra_data,
            }.items() if v is not None
        }
        self.logger.log(level, log_data)

    # --- cache helpers (sync, delegated) ------------------------------------

    def dump_cache(self, dest_fp=None):
        """Dump the request cache to a ``dict`` or the specified filepath.

        Serialises the request/response pairs stored in the handler's
        per-station caches.  If *dest_fp* is ``None`` the ``dict`` is
        returned directly; otherwise it is pickled to disk.

        Args:
            dest_fp: The destination filepath.  If ``None``, the cache
                contents are returned as a ``dict``.

        Returns:
            The cached request/response pairs as a ``dict``, or ``None``
            if *dest_fp* is specified.
        """
        data = dict()
        if self._handler:
            ids = [r.id_ for r in self._handler.stations]
            caches = [r.reqs.cache for r in self._handler.stations]
            for station_id, cache in zip(ids, caches):
                data[station_id] = dict()
                for req in cache.keys():
                    data[station_id][req] = cache[req].v
        if dest_fp:  # pragma: no cover
            with open(dest_fp, 'wb') as f:
                pickle.dump(data, f)
        else:
            return data

    def clear_cache(self):
        """Clear all cached requests and station state."""
        if self._handler:
            self._handler.stations = []

    def set_cache_limit(self, new_limit: int):
        """Set the per-station LRU cache limit.

        Args:
            new_limit: The new maximum number of cached responses per
                station.
        """
        self.cache_limit = new_limit
        if self._handler:
            self._handler.set_cache_limit(new_limit)

    def get_cache_limit(self) -> int:
        """Get the per-station LRU cache limit.

        Returns:
            The current cache limit.
        """
        return self.cache_limit

    def get_headers(self) -> dict:
        """Return the current headers used by the request handler."""
        if self._handler:
            return self._handler.get_headers()
        return self.headers  # pragma: no cover

    def update_headers(self, new: dict):
        """Add new headers to the request handler.

        Args:
            new: A ``dict`` of headers to merge into the existing set.
        """
        self.headers.update(new)
        if self._handler:
            self._handler.update_headers(new)

    def set_headers(self, request_headers: dict):
        """Replace all request headers with the supplied headers.

        Args:
            request_headers: The new complete set of request headers.
        """
        self.headers = request_headers
        if self._handler:
            self._handler.set_headers(request_headers)

    # --- station methods (public async) ------------------------------------

    async def stations(self, as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get all stations and station metadata from the NDBC.

        Query the NDBC data service for the current available data buoys
        (stations), both those maintained by the NDBC and those whose
        measurements are managed by the NDBC.

        Args:
            as_df: If ``True`` (default), return a ``pandas.DataFrame``.
                If ``False``, return a ``dict``.

        Returns:
            The current station data from the NDBC data service.

        Raises:
            ResponseException: An error occurred while retrieving and
                parsing responses from the NDBC data service.
        """
        try:
            req = ActiveStationsRequest.build_request()
            resp = await self._handler.handle_request('stn_active', req)
            data = ActiveStationsParser.df_from_response(resp,
                                                         use_timestamp=False)
            return handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    async def historical_stations(
            self, as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get historical stations and station metadata from the NDBC.

        Query the NDBC data service for the historical data buoys
        (stations), returned by default as rows of a
        ``pandas.DataFrame`` with one row per (station, deployment).

        Args:
            as_df: If ``True`` (default), return a ``pandas.DataFrame``.
                If ``False``, return a ``dict``.

        Returns:
            The historical station data from the NDBC data service.

        Raises:
            ResponseException: An error occurred while retrieving and
                parsing responses from the NDBC data service.
        """
        try:
            req = HistoricalStationsRequest.build_request()
            resp = await self._handler.handle_request('stn_historical', req)
            data = HistoricalStationsParser.df_from_response(
                resp, use_timestamp=False)
            return handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    async def nearest_station(
        self,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
    ) -> str:
        """Get the nearest station to the specified lat/lon.

        Use the NDBC data service's current station data to determine
        the nearest station to the specified latitude and longitude
        (either as ``float`` or as DD.dd[N/S/E/W] strings).

        Args:
            lat: The latitude of interest.
            lon: The longitude of interest.

        Returns:
            The station ID (e.g. ``'tplm2'``) of the nearest station.

        Raises:
            ValueError: *lat* and *lon* were not both specified.
            ParserException: An error occurred when computing distance.
        """
        if not (lat and lon):
            raise ValueError('lat and lon must be specified.')
        df = await self.stations(as_df=True)
        if isinstance(lat, str):
            lat = StationsHandler.LAT_MAP(lat)
        if isinstance(lon, str):
            lon = StationsHandler.LON_MAP(lon)
        try:
            closest = StationsHandler._nearest(df, lat, lon)
        except (TypeError, KeyError, ValueError) as e:
            raise ParserException from e
        closest = closest.to_dict().get('Station', {'UNK': 'UNK'})
        return list(closest.values())[0]

    async def radial_search(
        self,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
        radius: float = -1,
        units: str = 'km',
    ) -> pd.DataFrame:
        """Get all stations within *radius* of the specified lat/lon.

        Args:
            lat: The latitude of interest.
            lon: The longitude of interest.
            radius: The search radius in the specified *units*.
            units: One of ``'km'``, ``'nm'``, or ``'mi'``.

        Returns:
            A ``pandas.DataFrame`` of the stations within the radius.

        Raises:
            ValueError: Invalid *lat*/*lon*, *radius*, or *units*.
            ParserException: An error occurred computing distances.
        """
        if not (lat and lon):
            raise ValueError('lat and lon must be specified.')
        if units not in StationsHandler.UNITS:
            raise ValueError(
                f'Invalid unit: {units}, must be one of {StationsHandler.UNITS}.'
            )
        if radius < 0:
            raise ValueError(
                f'Invalid radius: {radius}, must be non-negative.')
        if units == 'nm':
            radius = radius * 1.852
        elif units == 'mi':
            radius = radius * 1.60934

        df = await self.stations(as_df=True)
        if isinstance(lat, str):
            lat = StationsHandler.LAT_MAP(lat)
        if isinstance(lon, str):
            lon = StationsHandler.LON_MAP(lon)
        try:
            return StationsHandler._radial_search(df, lat, lon, radius)
        except (TypeError, KeyError, ValueError) as e:
            raise ParserException from e

    async def station(
        self,
        station_id: Union[str, int],
        as_df: bool = False,
    ) -> Union[pd.DataFrame, dict]:
        """Get metadata for the given station from the NDBC.

        Args:
            station_id: The NDBC station ID (e.g. ``'tplm2'`` or
                ``41001``).
            as_df: If ``True``, return a ``pandas.DataFrame``.
                Defaults to ``False`` (returns a ``dict``).

        Returns:
            The station metadata.

        Raises:
            ResponseException: An error occurred when requesting and
                parsing responses for the specified station.
        """
        station_id = parse_station_id(station_id)
        try:
            req = MetadataRequest.build_request(station_id=station_id)
            resp = await self._handler.handle_request(station_id, req)
            data = MetadataParser.metadata(resp)
            return handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:  # pragma: no cover
            raise ResponseException('Failed to handle returned data.') from e

    async def available_realtime(
        self,
        station_id: Union[str, int],
        full_response: bool = False,
        as_df: Optional[bool] = None,
    ) -> Union[List[str], pd.DataFrame, dict]:
        """Get the available realtime modalities for a station.

        Queries the NDBC station webpage for the measurements which are
        currently available or were available over the last 45 days.

        Args:
            station_id: The NDBC station ID.
            full_response: If ``True``, return the full URL and
                description for each data mode.  If ``False``
                (default), return only the list of matching mode names.
            as_df: If ``True``, return a ``pandas.DataFrame`` when
                *full_response* is ``True``.  Defaults to ``None``
                (returns a ``dict`` for full responses, or a ``list``
                of mode names when *full_response* is ``False``).

        Returns:
            A ``list`` of available mode names, or a ``dict`` /
            ``pandas.DataFrame`` with full details when
            *full_response* is ``True``.

        Raises:
            ResponseException: An error occurred when requesting and
                parsing responses for the specified station.
        """
        station_id = parse_station_id(station_id)
        try:
            req = RealtimeRequest.build_request(station_id=station_id)
            resp = await self._handler.handle_request(station_id, req)
            station_realtime = RealtimeParser.available_measurements(resp)
            if full_response:
                if as_df is None:
                    as_df = False
                return handle_data(station_realtime, as_df, cols=None)
            full_data = handle_data(station_realtime,
                                    as_df=False,
                                    cols=None)
            _modes = self.get_modes()
            station_modes = set()
            for k in full_data:
                for m in _modes:
                    if m in full_data[k]['description']:
                        station_modes.add(m)
            return list(station_modes)
        except (ResponseException, ValueError, KeyError) as e:  # pragma: no cover
            raise ResponseException('Failed to handle returned data.') from e

    async def available_historical(
        self,
        station_id: Union[str, int],
        as_df: bool = False,
    ) -> Union[pd.DataFrame, dict]:
        """Get the available historical measurements for a station.

        Queries the NDBC station webpage for historical, quality-
        controlled measurements and their associated availability
        time ranges.

        Args:
            station_id: The NDBC station ID.
            as_df: If ``True``, return a ``pandas.DataFrame``.
                Defaults to ``False`` (returns a ``dict``).

        Returns:
            The available historical measurements for the specified
            station alongside their NDBC data links.

        Raises:
            ResponseException: An error occurred when requesting and
                parsing responses for the specified station.
        """
        station_id = parse_station_id(station_id)
        try:
            req = HistoricalRequest.build_request(station_id=station_id)
            resp = await self._handler.handle_request(station_id, req)
            data = HistoricalParser.available_measurements(resp)
            return handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    # --- data retrieval (public async) -------------------------------------

    async def get_data(
        self,
        station_id: Union[int, str, None] = None,
        mode: Union[str, None] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
        cols: List[str] = None,
        station_ids: Union[Sequence[Union[int, str]], None] = None,
        modes: Union[List[str], None] = None,
        as_xarray_dataset: bool = False,
        use_opendap: Optional[bool] = None,
    ) -> Union[pd.DataFrame, xarray.Dataset, dict]:
        """Execute a data query against the specified NDBC station(s).

        Query the NDBC data service for station-level measurements,
        using the *mode* parameter to determine the measurement type
        (e.g. ``'stdmet'`` for standard meteorological data).  When
        *station_ids* is supplied, queries for each station are
        executed concurrently via ``asyncio.gather``.

        Args:
            station_id: A single NDBC station ID.
            station_ids: A list of NDBC station IDs for concurrent
                multi-station queries.
            mode: The measurement type (e.g. ``'stdmet'``, ``'cwind'``).
            modes: A list of measurement types.
            start_time: The start of the time range (UTC).
            end_time: The end of the time range (UTC).
            use_timestamp: Whether to parse timestamp columns and set
                them as the index.
            as_df: If ``True`` (default), return a
                ``pandas.DataFrame``.  Ignored when
                *as_xarray_dataset* is ``True``.
            cols: Optional column selection.
            as_xarray_dataset: If ``True``, return an
                ``xarray.Dataset`` via the THREDDS/OpenDAP service.
            use_opendap: Alias for *as_xarray_dataset*.

        Returns:
            The station measurements as a ``pandas.DataFrame``,
            ``xarray.Dataset``, or ``dict``.

        Raises:
            ValueError: Invalid station/mode argument combinations.
            RequestException: The specified mode is not available.
            ResponseException: There was an error executing requests.
            HandlerException: There was an error handling returned data.
        """
        if use_opendap is not None:
            as_xarray_dataset = use_opendap

        as_df = as_df and not as_xarray_dataset

        self.log(logging.DEBUG,
                 message=f"`get_data` called with arguments: {locals()}")

        # --- validate inputs -----------------------------------------------
        if station_id is None and station_ids is None:
            raise ValueError('Both `station_id` and `station_ids` are `None`.')
        if station_id is not None and station_ids is not None:
            raise ValueError(
                '`station_id` and `station_ids` cannot both be specified.')
        if modes is not None:
            if not isinstance(modes, list):
                raise ValueError('`modes` must be a list of strings.')
            if any(not isinstance(m, str) for m in modes):
                raise ValueError('All elements in `modes` must be strings.')
            if any(m == 'hfradar' for m in modes):
                raise ValueError(
                    'HF radar data cannot be requested with `modes`. '
                    'Please use `mode` to specify a single `hfradar` mode.')
        if mode is None and modes is None:
            raise ValueError('Both `mode` and `modes` are `None`.')
        if mode is not None and modes is not None:
            raise ValueError(
                '`mode` and `modes` cannot both be specified.')

        handle_station_ids: List[Union[int, str]] = []
        handle_modes: List[str] = []

        if station_id is not None:
            handle_station_ids.append(station_id)
        if station_ids is not None:
            handle_station_ids.extend(station_ids)
        if mode is not None:
            handle_modes.append(mode)
        if modes is not None:
            handle_modes.extend(modes)

        for m in handle_modes:
            if m not in self.get_modes(use_opendap=as_xarray_dataset):
                raise RequestException(f"Mode {m} is not available.")

        self.log(logging.INFO,
                 message=(f"Processing request for station_ids "
                          f"{handle_station_ids} and modes "
                          f"{handle_modes}"))

        # --- concurrent station fetch per mode ------------------------------
        accumulated_data: Dict[str, list] = {}
        for m in handle_modes:
            accumulated_data[m] = []

            tasks = [
                self._async_handle_get_data(
                    mode=m,
                    station_id=sid,
                    start_time=start_time,
                    end_time=end_time,
                    use_timestamp=use_timestamp,
                    as_df=as_df,
                    cols=cols,
                    use_opendap=as_xarray_dataset,
                )
                for sid in handle_station_ids
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    if isinstance(
                            result,
                        (RequestException, ResponseException,
                         HandlerException)):
                        self.log(
                            level=logging.WARN,
                            message=f"Failed to process request: {result}",
                        )
                    else:
                        raise result
                else:
                    station_data, sid = result
                    self.log(level=logging.DEBUG,
                             station_id=sid,
                             message=(f"Successfully processed request "
                                      f"for station_id {sid}"))
                    if as_df:
                        station_data['station_id'] = sid
                    accumulated_data[m].append(station_data)

        self.log(logging.INFO, message="Finished processing request.")
        return handle_accumulate_data(
            accumulated_data,
            as_xarray_dataset=as_xarray_dataset,
        )

    def get_modes(
        self,
        use_opendap: bool = False,
        as_xarray_dataset: Optional[bool] = None,
    ) -> List[str]:
        """Get the list of supported measurement modes.

        Args:
            use_opendap: If ``True``, return modes available via the
                THREDDS/OpenDAP service.
            as_xarray_dataset: Alias for *use_opendap*.

        Returns:
            A ``list`` of mode name strings.
        """
        if as_xarray_dataset is not None:
            use_opendap = as_xarray_dataset
        if use_opendap:
            return list(self._OPENDAP_DISPATCH.keys())
        return list(self._HTTP_DISPATCH.keys())

    @staticmethod
    def save_xarray_dataset(dataset: xarray.Dataset, output_filepath: str,
                            **kwargs):
        """Save an ``xarray.Dataset`` to netCDF.

        Args:
            dataset: The ``xarray.Dataset`` to save.
            output_filepath: The destination filepath.
            **kwargs: Additional keyword arguments passed to
                :meth:`xarray.Dataset.to_netcdf`.
        """
        dataset.to_netcdf(output_filepath, **kwargs)  # pragma: no cover

    # --- private async dispatch --------------------------------------------

    async def _async_handle_get_data(
        self,
        mode: str,
        station_id: Union[int, str],
        start_time: Union[str, datetime],
        end_time: Union[str, datetime],
        use_timestamp: bool,
        as_df: bool = True,
        cols: List[str] = None,
        use_opendap: bool = False,
    ) -> Tuple[Union[pd.DataFrame, xarray.Dataset, dict], str]:
        """Async version of :meth:`NdbcApi._handle_get_data`.

        Directly composes request builders -> async handler -> parsers,
        bypassing the synchronous handler classmethods.
        """
        start_time = handle_timestamp(start_time)
        end_time = handle_timestamp(end_time)
        station_id = parse_station_id(station_id)

        dispatch = (self._OPENDAP_DISPATCH
                    if use_opendap else self._HTTP_DISPATCH)
        entry = dispatch.get(mode)
        if not entry:
            raise RequestException(
                'Please supply a supported mode from `get_modes()`.')
        RequestBuilder, Parser = entry

        # 1. Build request URLs (sync — pure CPU)
        try:
            reqs = RequestBuilder.build_request(station_id=station_id,
                                                start_time=start_time,
                                                end_time=end_time)
        except Exception as e:  # pragma: no cover
            raise RequestException('Failed to build request.') from e

        # 2. Execute requests (async — I/O)
        try:
            resps = await self._handler.handle_requests(
                station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e

        # 3. Parse responses (sync — CPU)
        try:
            if use_opendap:
                data = Parser.nc_from_responses(responses=resps,
                                                use_timestamp=use_timestamp)
            else:
                data = Parser.df_from_responses(responses=resps,
                                                use_timestamp=use_timestamp)
        except Exception as e:  # pragma: no cover
            raise ResponseException(
                f'Failed to parse responses.\nRaised from {e}') from e

        # 4. Post-process: time range enforcement and column selection
        if use_timestamp:
            if use_opendap:
                data = filter_dataset_by_time_range(data, start_time, end_time)
            else:
                data = enforce_timerange(df=data,
                                         start_time=start_time,
                                         end_time=end_time)
        try:
            if use_opendap:
                handled_data = (filter_dataset_by_variable(data, cols)
                                if cols else data)
            else:
                handled_data = handle_data(data, as_df, cols)
        except (ValueError, KeyError, AttributeError) as e:  # pragma: no cover
            raise ParserException(
                f'Failed to handle returned data.\nRaised from {e}') from e

        return (handled_data, station_id)
