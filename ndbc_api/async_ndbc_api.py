"""An async API for retrieving data from the NDBC.

This module defines the `AsyncNdbcApi`, which mirrors the public API
of `NdbcApi` with `async def` methods backed by `aiohttp` for
non-blocking I/O and `asyncio.gather` for concurrent multi-station
queries.

Example:
    ```python3
    from ndbc_api import AsyncNdbcApi

    async with AsyncNdbcApi() as api:
        available_stations = await api.stations()
        modes = api.get_modes()
        df_stdmet_tplm2 = await api.get_data(
            'tplm2',
            'stdmet',
            '2020-01-01',
            '2022-01-01',
            as_df=True
        )
    ```

Attributes:
    log (:obj:`logging.Logger`): The logger at which to register HTTP
        request and response status codes and headers used for debug
        purposes.
    headers (:dict:): The request headers for use in the async API's
        request handler.
"""
import asyncio
import logging
import os
import pickle
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import polars as pl
except ImportError:
    pl = None
try:
    import xarray
except ImportError:
    xarray = None

from .api.dispatch import HTTP_DISPATCH, OPENDAP_DISPATCH, build_mode_requests
from .api.handlers.http.stations import StationsHandler
from .api.parsers.http.active_stations import ActiveStationsParser
from .api.parsers.http.historical_stations import HistoricalStationsParser
from .api.parsers.http.station_historical import HistoricalParser
from .api.parsers.http.station_metadata import MetadataParser
from .api.parsers.http.station_realtime import RealtimeParser
from .api.requests.http.active_stations import ActiveStationsRequest
from .api.requests.http.historical_stations import HistoricalStationsRequest
from .api.requests.http.station_historical import HistoricalRequest
from .api.requests.http.station_metadata import MetadataRequest
from .api.requests.http.station_realtime import RealtimeRequest
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
from .utilities.data_helpers import (
    enforce_timerange as _enforce_timerange_impl,
    extract_realtime_modes as _extract_realtime_modes_impl,
    filter_station_deployments as _filter_station_deployments_impl,
    handle_accumulate_data as _handle_accumulate_data_impl,
    handle_data as _handle_data_impl,
    handle_timestamp as _handle_timestamp_impl,
    parse_station_id as _parse_station_id_impl,
    summarize_historical_modes as _summarize_historical_modes_impl,
)
from .utilities.log_formatter import LogFormatter
from .utilities.mode_metadata import describe_mode as _describe_mode_impl
from .utilities.opendap.dataset import (
    filter_dataset_by_time_range,
    filter_dataset_by_variable,
)


class AsyncNdbcApi:
    """Async API for retrieving data from the NDBC.

    Provides the same public API surface as `NdbcApi`, but with `async def`
    methods backed by `aiohttp` for non-blocking I/O and `asyncio.gather`
    for concurrent multi-station queries.

    Unlike `NdbcApi`, this class is not a singleton. Callers manage its
    lifecycle via `async with`:

    ```python3
    async with AsyncNdbcApi() as api:
        df = await api.stations()
    ```

    Attributes:
        logger (:obj:`logging.Logger`): The logger at which to register
            HTTP request and response status codes.
        headers (:dict:): The request headers for use in the API's async
            request handler.
        cache_limit: The handler's per-station LRU cache limit.
    """

    _HTTP_DISPATCH = HTTP_DISPATCH
    _OPENDAP_DISPATCH = OPENDAP_DISPATCH

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
        """Initialize the `AsyncNdbcApi` and configure logging.

        Args:
            logging_level: The `logging.Logger`'s log level.
            filename: If provided, logs are written to this filepath
                instead of `stderr`.
            cache_limit: The per-station LRU cache limit for the
                underlying `AsyncRequestHandler`.
            headers: Optional HTTP headers to send with each request.
            delay: The inter-request delay in milliseconds.
            retries: The maximum number of retry attempts per request.
            backoff_factor: The exponential back-off factor applied
                between retries.
            verify_https: Whether to verify TLS certificates.
            debug: If `True`, enables verbose logging.
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

    async def __aenter__(self) -> 'AsyncNdbcApi':
        """Enter the async runtime context and initialize the HTTP session."""
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
        """Exit the async runtime context and close the HTTP session."""
        if self._handler:
            await self._handler.__aexit__(exc_type, exc_val, exc_tb)

    def dump_cache(self, dest_fp: Union[str, os.PathLike, None] = None):
        """Dump the request cache to a `dict` or the specified filepath.

        Serializes the request/response pairs stored in the handler's
        per-station caches. If `dest_fp` is `None` the `dict` is
        returned directly; otherwise it is pickled to disk.

        Args:
            dest_fp: The destination filepath. If `None`, the cache
                contents are returned as a `dict`.

        Returns:
            The cached request/response pairs as a `dict`, or `None`
            if `dest_fp` is specified.
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

    def load_cache(self, src: Union[dict, str, os.PathLike]) -> None:
        """Load cached request/response pairs from a `dict` or pickle filepath.

        Counterpart to `dump_cache`. Populates the per-station LRU caches
        without network I/O.

        Args:
            src: A `dict` mapping `station_id` to `{url: response_dict}` as
                returned by `dump_cache`, or a filepath (`str` or `Path`)
                to a pickled cache dictionary.

        Raises:
            ValueError: If `src` is not a `dict` or a pickle file containing
                a `dict`.
        """
        if isinstance(src, dict):
            data = src
        elif isinstance(src, (str, os.PathLike)):
            with open(src, 'rb') as f:
                data = pickle.load(f)
            if not isinstance(data, dict):
                raise ValueError('Pickled cache must deserialize to a dict.')
        else:
            raise ValueError('`src` must be a dict or a filepath to a pickled cache.')

        if self._handler is None:
            return
        for station_id, req_map in data.items():
            if not isinstance(req_map, dict):
                continue
            stn = self._handler.get_station(station_id)
            for req_url, resp in req_map.items():
                stn.reqs.put(request=req_url, response=resp)

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
            new: A `dict` of headers to merge into the existing set.
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

    def configure_logging(self, level=logging.WARNING, filename=None):
        """Configure logging for the `AsyncNdbcApi`.

        Args:
            level (int, optional): The logging level. Defaults to `logging.WARNING`.
            filename (str, optional): If provided, logs are written to the
                specified file.
        """
        self.logger.setLevel(level)
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            if isinstance(handler, logging.FileHandler):
                handler.close()
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
            level (int): The logging level.
            station_id (str, optional): The NDBC station ID.
            mode (str, optional): The data mode.
            message (str, optional): The log message.
            **extra_data: Additional key-value pairs to include in the log.
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

    async def stations(self, as_df: bool = True, as_pl: bool = False) -> Any:
        """Get all stations and station metadata from the NDBC.

        Query the NDBC data service for the current available data buoys
        (stations), both those maintained by the NDBC and those whose
        measurements are managed by the NDBC.

        Args:
            as_df: Flag indicating whether to return current station data as a
                `pandas.DataFrame` if set to `True` or as a `dict` if `False`.
            as_pl: Flag indicating whether to return current station data as a
                `polars.DataFrame` if set to `True`.

        Returns:
            The current station data from the NDBC data service, either as a
            `pandas.DataFrame`, `polars.DataFrame`, or as a `dict`.

        Raises:
            ResponseException: An error occurred while retrieving and
                parsing responses from the NDBC data service.
        """
        try:
            req = ActiveStationsRequest.build_request()
            resp = await self._handler.handle_request('stn_active', req)
            data = ActiveStationsParser.parse_response(resp,
                                                       use_timestamp=False)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError, AttributeError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    async def historical_stations(
            self, as_df: bool = True, as_pl: bool = False) -> Any:
        """Get historical stations and station metadata from the NDBC.

        Query the NDBC data service for the historical data buoys
        (stations), returned by default as rows of a
        `pandas.DataFrame` with one row per (station, deployment).

        Args:
            as_df: Flag indicating whether to return station data as a
                `pandas.DataFrame` if set to `True` or as a `dict` if `False`.
            as_pl: Flag indicating whether to return station data as a
                `polars.DataFrame` if set to `True`.

        Returns:
            The historical station data from the NDBC data service, either as a
            `pandas.DataFrame`, `polars.DataFrame`, or as a `dict`.

        Raises:
            ResponseException: An error occurred while retrieving and
                parsing responses from the NDBC data service.
        """
        try:
            req = HistoricalStationsRequest.build_request()
            resp = await self._handler.handle_request('stn_historical', req)
            data = HistoricalStationsParser.parse_response(
                resp, use_timestamp=False)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError, AttributeError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    async def nearest_station(
        self,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
        met: Optional[bool] = None,
        currents: Optional[bool] = None,
        waterquality: Optional[bool] = None,
        dart: Optional[bool] = None,
        owner: Optional[str] = None,
        program: Optional[str] = None,
        station_type: Optional[str] = None,
        k: int = 1,
    ) -> Union[str, List[str]]:
        """Get the nearest station(s) to the specified lat/lon with optional capability filters.

        Args:
            lat: The latitude of interest.
            lon: The longitude of interest.
            met: Optional filter for `'Includes Meteorology'`.
            currents: Optional filter for `'Includes Currents'`.
            waterquality: Optional filter for `'Includes Water Quality'`.
            dart: Optional filter for `'DART Program'`.
            owner: Optional case-insensitive substring filter for `'Owner'`.
            program: Optional case-insensitive substring filter for `'Program'`.
            station_type: Optional case-insensitive substring filter for `'Type'`.
            k: Number of nearest stations to return. Defaults to `1` (returns a
                `str` station ID); when `k > 1`, returns a `List[str]`.

        Returns:
            The station ID (when `k == 1`) or list of station IDs (when `k > 1`).

        Raises:
            ValueError: If `lat` and `lon` are not both specified, or `k < 1`.
            ParserException: An error occurred when computing distance.
        """
        if lat is None or lon is None:
            raise ValueError('lat and lon must be specified.')
        if k < 1:
            raise ValueError('k must be >= 1.')
        lat_f = StationsHandler.LAT_MAP(lat)
        lon_f = StationsHandler.LON_MAP(lon)
        df = await self.stations(as_df=False, as_pl=False)
        return StationsHandler.nearest_station(
            lat=lat_f,
            lon=lon_f,
            met=met,
            currents=currents,
            waterquality=waterquality,
            dart=dart,
            owner=owner,
            program=program,
            station_type=station_type,
            k=k,
            stations=df,
        )

    async def radial_search(
        self,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
        radius: float = -1,
        units: str = 'km',
        as_df: bool = True,
        as_pl: bool = False,
        met: Optional[bool] = None,
        currents: Optional[bool] = None,
        waterquality: Optional[bool] = None,
        dart: Optional[bool] = None,
        owner: Optional[str] = None,
        program: Optional[str] = None,
        station_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Any:
        """Get all stations within `radius` of the specified lat/lon with optional filters.

        Args:
            lat: The latitude of interest.
            lon: The longitude of interest.
            radius: The search radius in the specified `units`.
            units: One of `'km'`, `'nm'`, or `'mi'`.
            as_df: If `True` (default), return a `pandas.DataFrame`.
            as_pl: If `True`, return a `polars.DataFrame`.
            met: Optional filter for `'Includes Meteorology'`.
            currents: Optional filter for `'Includes Currents'`.
            waterquality: Optional filter for `'Includes Water Quality'`.
            dart: Optional filter for `'DART Program'`.
            owner: Optional case-insensitive substring filter for `'Owner'`.
            program: Optional case-insensitive substring filter for `'Program'`.
            station_type: Optional case-insensitive substring filter for `'Type'`.
            limit: Optional maximum number of closest stations to return.

        Returns:
            A `pandas.DataFrame`, `polars.DataFrame`, or `dict` of the stations within the radius.
        """
        if lat is None or lon is None:
            raise ValueError('lat and lon must be specified.')
        if units not in StationsHandler.UNITS:
            raise ValueError(
                f'Invalid unit: {units}, must be one of {StationsHandler.UNITS}.'
            )
        if radius < 0:
            raise ValueError(
                f'Invalid radius: {radius}, must be non-negative.')
        if limit is not None and limit < 1:
            raise ValueError('limit must be >= 1.')
        lat_f = StationsHandler.LAT_MAP(lat)
        lon_f = StationsHandler.LON_MAP(lon)
        df = await self.stations(as_df=False, as_pl=False)
        stations_in_radius = StationsHandler.radial_search(
            lat=lat_f,
            lon=lon_f,
            radius=radius,
            units=units,
            met=met,
            currents=currents,
            waterquality=waterquality,
            dart=dart,
            owner=owner,
            program=program,
            station_type=station_type,
            limit=limit,
            stations=df,
        )
        return self._handle_data(stations_in_radius, as_df=as_df, as_pl=as_pl, cols=None)

    async def station_deployments(
        self,
        station_id: Union[str, int],
        start_time: Any = None,
        end_time: Any = None,
        as_df: bool = True,
        as_pl: bool = False,
    ) -> Any:
        """Get historical deployment and sensor height records for a station.

        Queries `stationmetadata.xml` (via `historical_stations`) and filters
        to deployments for `station_id`, optionally restricted to deployments
        overlapping `[start_time, end_time]`.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).
            start_time: Optional start of the time window.
            end_time: Optional end of the time window.
            as_df: If `True` (default), return a `pandas.DataFrame`.
            as_pl: If `True`, return a `polars.DataFrame`.

        Returns:
            Deployment records containing `'Start Date'`, `'End Date'`,
            `'Anemometer Height'`, `'Hull Type'`, `'Lat'`, `'Lon'`, and
            `'Elevation'`.
        """
        records = await self.historical_stations(as_df=False, as_pl=False)
        filtered = _filter_station_deployments_impl(
            records=records,
            station_id=station_id,
            start_time=start_time,
            end_time=end_time,
        )
        return self._handle_data(filtered, as_df=as_df, as_pl=as_pl, cols=None)

    async def explain_station(self, station_id: Union[str, int]) -> Dict[str, Any]:
        """Return a unified dossier of metadata, realtime modes, and historical coverage for a station.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).

        Returns:
            A `dict` containing `'station_id'`, `'metadata'`, `'realtime_modes'`,
            and `'historical_modes'`.
        """
        sid = self._parse_station_id(station_id)
        meta, rt_modes, hist_modes = await asyncio.gather(
            self.station(station_id=sid, as_df=False, as_pl=False),
            self.available_realtime(station_id=sid, full_response=False),
            self.available_historical(
                station_id=sid,
                full_response=False,
                as_df=False,
                as_pl=False,
            ),
        )
        return {
            'station_id': sid,
            'metadata': meta,
            'realtime_modes': rt_modes,
            'historical_modes': hist_modes,
        }

    async def station(
        self,
        station_id: Union[str, int],
        as_df: bool = False,
        as_pl: bool = False,
    ) -> Any:
        """Get metadata for the given station from the NDBC.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.

        Returns:
            The station metadata for the given station, either as a `dict`,
            `pandas.DataFrame`, or `polars.DataFrame`.

        Raises:
            ResponseException: An error occurred when requesting and
                parsing responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            req = MetadataRequest.build_request(station_id=station_id)
            resp = await self._handler.handle_request(station_id, req)
            data = MetadataParser.metadata(resp)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError) as e:  # pragma: no cover
            raise ResponseException('Failed to handle returned data.') from e

    async def available_realtime(
        self,
        station_id: Union[str, int],
        full_response: bool = False,
        as_df: Optional[bool] = None,
        as_pl: bool = False,
    ) -> Any:
        """Get the available realtime modalities for a station.

        Queries the NDBC station webpage for the measurements which are
        currently available or were available over the last 45 days.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).
            full_response: Whether to return the full response from the NDBC
                API, defaults to `False` and a list of modes from `get_modes()`
                is returned. If `True`, the full URL for each data mode is
                included in the returned `dict` or `pandas.DataFrame`.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.

        Returns:
            A `list` of available mode names, or a `dict`, `pandas.DataFrame`,
            or `polars.DataFrame` with full details when `full_response` is `True`.

        Raises:
            ResponseException: An error occurred when requesting and
                parsing responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            req = RealtimeRequest.build_request(station_id=station_id)
            resp = await self._handler.handle_request(station_id, req)
            station_realtime = RealtimeParser.available_measurements(resp)
            if full_response:
                if as_df is None and not as_pl:
                    as_df = False
                return self._handle_data(station_realtime, as_df=as_df, as_pl=as_pl, cols=None)
            full_data = self._handle_data(station_realtime,
                                          as_df=False,
                                          as_pl=False,
                                          cols=None)
            return _extract_realtime_modes_impl(full_data, self.get_modes())
        except (ResponseException, ValueError, KeyError) as e:  # pragma: no cover
            raise ResponseException('Failed to handle returned data.') from e

    async def available_historical(
        self,
        station_id: Union[str, int],
        as_df: bool = False,
        as_pl: bool = False,
        full_response: bool = True,
    ) -> Any:
        """Get the available historical measurements for a station.

        Queries the NDBC station webpage for historical, quality-
        controlled measurements and their associated availability
        time ranges.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.
            full_response: If `True` (default), return the full nested
                dictionary of historical URLs. If `False`, return a
                dictionary mapping canonical `ndbc-api` mode names to
                their available historical periods.

        Returns:
            The available historical measurements for the specified
            station alongside their NDBC data links, either as a `dict`,
            `pandas.DataFrame`, or `polars.DataFrame`.

        Raises:
            ResponseException: An error occurred when requesting and
                parsing responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            req = HistoricalRequest.build_request(station_id=station_id)
            resp = await self._handler.handle_request(station_id, req)
            data = HistoricalParser.available_measurements(resp)
            if not full_response:
                summary = _summarize_historical_modes_impl(data, self.get_modes())
                if not as_df and not as_pl:
                    return summary
                return self._handle_data(summary, as_df=as_df, as_pl=as_pl, cols=None)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def build_requests(
        self,
        station_id: Union[int, str, None] = None,
        mode: Union[str, None] = None,
        start_time: Union[str, datetime, None] = None,
        end_time: Union[str, datetime, None] = None,
        use_opendap: bool = False,
        as_xarray_dataset: Optional[bool] = None,
        station_ids: Union[Sequence[Union[int, str]], None] = None,
        modes: Union[List[str], None] = None,
    ) -> Union[List[str], Dict[str, Any]]:
        """Build the ordered list of NDBC/THREDDS archive URLs for a query without network I/O.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).
            mode: The measurement mode (e.g. `'stdmet'` or `'cwind'`).
            start_time: Start of the time range (defaults to 30 days ago).
            end_time: End of the time range (defaults to current time).
            use_opendap: If `True`, generate THREDDS/OpenDAP NetCDF URLs.
            as_xarray_dataset: An alias for `use_opendap`.
            station_ids: Sequence of NDBC station IDs for multi-station queries.
            modes: List of measurement modes for multi-mode queries.

        Returns:
            Ordered list of request URLs (when `station_id` and `mode` are given)
            or nested `dict` keyed by station/mode for batch queries.

        Raises:
            ValueError: If station or mode arguments are missing or mutually
                exclusive.
            RequestException: If `mode` is not supported.
            TimestampException: If `start_time` or `end_time` is invalid or
                `start_time > end_time`.
        """
        if station_id is None and station_ids is None:
            raise ValueError('Both `station_id` and `station_ids` are `None`.')
        if station_id is not None and station_ids is not None:
            raise ValueError('`station_id` and `station_ids` cannot both be specified.')
        if mode is None and modes is None:
            raise ValueError('Both `mode` and `modes` are `None`.')
        if mode is not None and modes is not None:
            raise ValueError('`mode` and `modes` cannot both be specified.')

        if as_xarray_dataset is not None:
            use_opendap = as_xarray_dataset
        now = datetime.now()
        start_dt = self._handle_timestamp(
            start_time if start_time is not None else now - timedelta(days=30)
        )
        end_dt = self._handle_timestamp(
            end_time if end_time is not None else now
        )
        if start_dt > end_dt:
            raise TimestampException(
                f'`start_time` ({start_dt}) must be <= `end_time` ({end_dt}).'
            )

        if station_id is not None and mode is not None:
            sid = self._parse_station_id(station_id)
            return build_mode_requests(
                mode=mode,
                station_id=sid,
                start_time=start_dt,
                end_time=end_dt,
                use_opendap=use_opendap,
            )

        sids = [self._parse_station_id(s) for s in (station_ids if station_ids is not None else [station_id])]
        ms = list(modes if modes is not None else [mode])

        if station_ids is not None and mode is not None:
            return {
                sid: build_mode_requests(
                    mode=mode,
                    station_id=sid,
                    start_time=start_dt,
                    end_time=end_dt,
                    use_opendap=use_opendap,
                )
                for sid in sids
            }
        if station_id is not None and modes is not None:
            return {
                m: build_mode_requests(
                    mode=m,
                    station_id=sids[0],
                    start_time=start_dt,
                    end_time=end_dt,
                    use_opendap=use_opendap,
                )
                for m in ms
            }
        return {
            sid: {
                m: build_mode_requests(
                    mode=m,
                    station_id=sid,
                    start_time=start_dt,
                    end_time=end_dt,
                    use_opendap=use_opendap,
                )
                for m in ms
            }
            for sid in sids
        }

    async def get_data(
        self,
        station_id: Union[int, str, None] = None,
        mode: Union[str, None] = None,
        start_time: Union[str, datetime, None] = None,
        end_time: Union[str, datetime, None] = None,
        use_timestamp: bool = True,
        as_df: bool = True,
        as_pl: bool = False,
        cols: List[str] = None,
        station_ids: Union[Sequence[Union[int, str]], None] = None,
        modes: Union[List[str], None] = None,
        as_xarray_dataset: bool = False,
        use_opendap: Optional[bool] = None,
    ) -> Any:
        """Execute a data query against the specified NDBC station(s).

        Query the NDBC data service for station-level measurements,
        using the `mode` parameter to determine the measurement type
        (e.g. `'stdmet'` for standard meteorological data). When
        `station_ids` is supplied, queries for each station are
        executed concurrently via `asyncio.gather`.

        Args:
            station_id: A single NDBC station ID.
            station_ids: A list of NDBC station IDs for concurrent
                multi-station queries.
            mode: The measurement type (e.g. `'stdmet'`, `'cwind'`).
            modes: A list of measurement types.
            start_time: The start of the time range (UTC).
            end_time: The end of the time range (UTC).
            use_timestamp: Whether to parse timestamp columns and set
                them as the index.
            as_df: If `True` (default), return a `pandas.DataFrame`.
                Ignored when `as_xarray_dataset` is `True`.
            as_pl: If `True`, return a `polars.DataFrame`.
            cols: Optional column selection.
            as_xarray_dataset: If `True`, return an `xarray.Dataset`
                via the THREDDS/OpenDAP service.
            use_opendap: An alias for `as_xarray_dataset`.

        Returns:
            The station measurements as a `pandas.DataFrame`,
            `polars.DataFrame`, `xarray.Dataset`, or `dict`.

        Raises:
            ValueError: Invalid station/mode argument combinations.
            RequestException: The specified mode is not available.
            ResponseException: There was an error executing requests.
            HandlerException: There was an error handling returned data.
        """
        now = datetime.now()
        if start_time is None:
            start_time = now - timedelta(days=30)
        if end_time is None:
            end_time = now

        if use_opendap is not None:
            as_xarray_dataset = use_opendap

        if as_xarray_dataset and xarray is None:
            raise ImportError(
                'xarray is required for OpenDAP support. If you uninstalled it '
                'to create a lightweight environment, you must reinstall it to use this feature.'
            )

        if as_pl:
            as_df = False
        as_df = as_df and not as_xarray_dataset

        self.log(logging.DEBUG,
                 message=f'`get_data` called with arguments: {locals()}')

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

        start_dt = self._handle_timestamp(start_time)
        end_dt = self._handle_timestamp(end_time)
        if start_dt > end_dt:
            raise TimestampException(
                f'`start_time` ({start_dt}) must be <= `end_time` ({end_dt}).'
            )

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
                raise RequestException(f'Mode {m} is not available.')

        self.log(logging.INFO,
                 message=(f'Processing request for station_ids '
                          f'{handle_station_ids} and modes '
                          f'{handle_modes}'))

        accumulated_data: Dict[str, list] = {}
        for m in handle_modes:
            accumulated_data[m] = []

            tasks = [
                self._async_handle_get_data(
                    mode=m,
                    station_id=sid,
                    start_time=start_dt,
                    end_time=end_dt,
                    use_timestamp=use_timestamp,
                    as_df=as_df,
                    as_pl=as_pl,
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
                            message=f'Failed to process request: {result}',
                        )
                    else:
                        raise result
                else:
                    station_data, sid = result
                    self.log(level=logging.DEBUG,
                             station_id=sid,
                             message=(f'Successfully processed request '
                                      f'for station_id {sid}'))
                    if not as_xarray_dataset:
                        # station_data is a list of dicts
                        for row in station_data:
                            row['station_id'] = sid
                    elif (
                        len(handle_station_ids) > 1
                        and xarray is not None
                        and isinstance(station_data, xarray.Dataset)
                    ):
                        station_data = station_data.assign_coords(
                            station_id=sid
                        )
                    accumulated_data[m].append(station_data)

        self.log(logging.INFO, message='Finished processing request.')
        return self._handle_accumulate_data(
            accumulated_data,
            as_df=as_df,
            as_pl=as_pl,
            as_xarray_dataset=as_xarray_dataset,
        )

    def get_modes(
        self,
        use_opendap: bool = False,
        as_xarray_dataset: Optional[bool] = None,
    ) -> List[str]:
        """Get the list of supported modes for `get_data(...)`.

        Args:
            use_opendap (bool): Whether to return the available
                modes for opendap `xarray.Dataset` data.
            as_xarray_dataset (bool): An alias for `use_opendap`.

        Returns:
            (List[str]) the available modalities.
        """
        if as_xarray_dataset is not None:
            use_opendap = as_xarray_dataset
        if use_opendap:
            return list(self._OPENDAP_DISPATCH.keys())
        return list(self._HTTP_DISPATCH.keys())

    def describe_mode(self, mode: Optional[str] = None) -> Dict[str, Any]:
        """Return structured metadata, column units, and sentinel values for NDBC modes.

        Args:
            mode: Optional mode name (e.g. `'stdmet'` or `'cwind'`). If `None`,
                returns a dictionary describing all supported modes.

        Returns:
            A `dict` containing the mode's description, column schemas (name,
            units, description, missing sentinels), and endpoint availability.

        Raises:
            RequestException: If `mode` is not a recognized NDBC mode.
        """
        return _describe_mode_impl(mode)

    @staticmethod
    def save_xarray_dataset(dataset: 'xarray.Dataset', output_filepath: str,
                            **kwargs) -> None:
        """Save an `xarray.Dataset` to netCDF at a user-specified file path.

        Args:
            dataset: The `xarray.Dataset` to save.
            output_filepath: The path to save the dataset to.
            **kwargs: Additional keyword arguments passed to
                `dataset.to_netcdf`.
        """
        if xarray is None:
            raise ImportError(
                'xarray is required for OpenDAP support. If you uninstalled it '
                'to create a lightweight environment, you must reinstall it to use this feature.'
            )
        if not isinstance(dataset, xarray.Dataset):
            raise ValueError(
                f'Expected an xarray.Dataset, got {type(dataset).__name__}. '
                'This can happen when get_data() returns an empty result. '
                'Check the logs for errors.'
            )
        dataset.to_netcdf(output_filepath, **kwargs)  # pragma: no cover

    """ PRIVATE """

    @staticmethod
    def _parse_station_id(station_id: Union[str, int]) -> str:
        """Parse station id."""
        return _parse_station_id_impl(station_id)

    @staticmethod
    def _handle_timestamp(timestamp: Union[datetime, str]) -> datetime:
        """Convert the specified timestamp to `datetime.datetime`."""
        return _handle_timestamp_impl(timestamp)

    @staticmethod
    def _enforce_timerange(df: Any, start_time: datetime,
                           end_time: datetime) -> Any:
        """Down-select to the data within the specified `datetime` range."""
        return _enforce_timerange_impl(df, start_time, end_time)

    @staticmethod
    def _handle_data(data: Any,
                     as_df: bool = True,
                     as_pl: bool = False,
                     cols: List[str] = None) -> Any:
        """Apply column down selection and return format handling."""
        return _handle_data_impl(data, as_df, as_pl, cols)

    def _handle_accumulate_data(
        self,
        accumulated_data: Dict[str, List[Any]],
        as_df: bool = True,
        as_pl: bool = False,
        as_xarray_dataset: bool = False,
    ) -> Any:
        """Accumulate the data from multiple stations and modes, coalescing overlapping data."""
        return _handle_accumulate_data_impl(
            accumulated_data,
            as_df=as_df,
            as_pl=as_pl,
            as_xarray_dataset=as_xarray_dataset,
        )

    async def _async_handle_get_data(
        self,
        mode: str,
        station_id: Union[int, str],
        start_time: Union[str, datetime],
        end_time: Union[str, datetime],
        use_timestamp: bool,
        as_df: bool = True,
        as_pl: bool = False,
        cols: List[str] = None,
        use_opendap: bool = False,
    ) -> Tuple[Any, str]:
        """Async version of `NdbcApi._handle_get_data`.

        Directly composes request builders, async handler, and parsers,
        bypassing the synchronous handler classmethods.
        """
        start_time = self._handle_timestamp(start_time)
        end_time = self._handle_timestamp(end_time)
        if start_time > end_time:
            raise TimestampException(
                f'`start_time` ({start_time}) must be <= `end_time` ({end_time}).'
            )
        station_id = self._parse_station_id(station_id)

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
                data = Parser.parse_responses(responses=resps,
                                              use_timestamp=use_timestamp)
        except Exception as e:  # pragma: no cover
            raise ResponseException(
                f'Failed to parse responses.\nRaised from {e}') from e

        # 4. Post-process: time range enforcement and column selection
        if use_timestamp:
            if use_opendap:
                data = filter_dataset_by_time_range(data, start_time, end_time)
            else:
                data = self._enforce_timerange(df=data,
                                               start_time=start_time,
                                               end_time=end_time)
        try:
            if use_opendap:
                handled_data = (filter_dataset_by_variable(data, cols)
                                if cols else data)
            else:
                # Keep it as raw list of dicts during accumulation to make merging easy!
                handled_data = self._handle_data(data, as_df=False, as_pl=False, cols=cols)
        except (ValueError, KeyError, AttributeError) as e:  # pragma: no cover
            raise ParserException(
                f'Failed to handle returned data.\nRaised from {e}') from e

        return (handled_data, station_id)
