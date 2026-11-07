"""An API for retrieving data from the NDBC.

This module defines the `NdbcApi`, the top-level object which creates, handles,
caches, parses, and returns NDBC data.

Example:
    ```python3
    from ndbc_api import NdbcApi
    api = NdbcApi()
    available_stations = api.stations()
    modes = api.get_modes()
    df_stdmet_tplm2 = api.get_data(
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
    headers(:dict:): The request headers for use in the NDBC API's request
        handler.
"""
import logging
import os

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, List, Sequence, Tuple, Union, Dict, Optional

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

from .api.dispatch import (
    HTTP_DISPATCH,
    OPENDAP_DISPATCH,
    build_requests as _build_requests_impl,
)
from .api.handlers.http.data import DataHandler
from .api.handlers.http.stations import StationsHandler
from .config import (DEFAULT_CACHE_LIMIT, HTTP_BACKOFF_FACTOR, HTTP_DEBUG,
                     HTTP_DELAY, HTTP_RETRY, LOGGER_NAME, VERIFY_HTTPS)
from .exceptions import (HandlerException, ParserException, RequestException,
                         ResponseException, TimestampException)
from .utilities.req_handler import RequestHandler
from .utilities.singleton import Singleton
from .utilities.log_formatter import LogFormatter
from .utilities.mode_metadata import describe_mode as _describe_mode_impl
from .utilities.data_helpers import (
    parse_station_id as _parse_station_id_impl,
    handle_timestamp as _handle_timestamp_impl,
    enforce_timerange as _enforce_timerange_impl,
    handle_data as _handle_data_impl,
    handle_accumulate_data as _handle_accumulate_data_impl,
    extract_realtime_modes as _extract_realtime_modes_impl,
    summarize_historical_modes as _summarize_historical_modes_impl,
    filter_station_deployments as _filter_station_deployments_impl,
    dump_handler_cache as _dump_handler_cache_impl,
    load_handler_cache as _load_handler_cache_impl,
)
from .api.handlers.opendap.data import OpenDapDataHandler
from .utilities.opendap.dataset import filter_dataset_by_variable, filter_dataset_by_time_range


class NdbcApi(metaclass=Singleton):
    """An API for querying the National Data Buoy Center.

    The `NdbcApi` is metaclassed as a singleton to conserve NDBC resources. It
    uses two private handlers to build requests and parse responses to the NDBC
    over HTTP(s). It also uses a LRU-cached request handler to execute requests
    against the NDBC, logging response statuses as they are executed.

    Attributes:
        logging_level: The `logging.Logger`s log level, 1 if the `debug`
            flag is set in the `__init__` method, and 0 otherwise.
        cache_limit: The handler's global limit for caching
            `NdbcApi` responses. This is implemented as a least-recently
            used cache, designed to conserve NDBC resources when querying
            measurements for a given station over similar time ranges.
        delay: The HTTP(s) request delay parameter, in seconds.
        retries: = The number of times to retry a request to the NDBC data
            service.
        backoff_factor: The back-off parameter, used in conjunction with
            `retries` to re-attempt requests to the NDBC data service.
        verify_https: A flag which indicates whether to attempt requests to the
            NDBC data service over HTTP or HTTPS.
        debug: A flag for verbose logging and response-level status reporting.
            Affects the instance's `logging.Logger` and the behavior of its
            private `RequestHandler` instance.
    """

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
        """Initializes the singleton `NdbcApi`, sets associated handlers."""
        self.cache_limit = cache_limit
        self.headers = headers or {}
        self._delay = delay
        self._retries = retries
        self._backoff_factor = backoff_factor
        self._verify_https = verify_https
        self._debug = debug
        self._handler = self._get_request_handler(
            cache_limit=self.cache_limit,
            delay=delay,
            retries=retries,
            backoff_factor=backoff_factor,
            headers=self.headers,
            debug=debug,
            verify_https=verify_https,
        )
        self._stations_api = StationsHandler
        self._data_api = DataHandler
        self._opendap_data_api = OpenDapDataHandler
        self.configure_logging(level=logging_level, filename=filename)

    def __enter__(self) -> 'NdbcApi':
        """Enter the runtime context related to the `NdbcApi` instance."""
        if getattr(self, '_handler', None) is not None and getattr(self._handler, '_closed', False):
            self._handler._reconfigure()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context and close the underlying HTTP session."""
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP session."""
        if getattr(self, '_handler', None) is not None:
            self._handler.close()

    def dump_cache(self, dest_fp: Union[str, os.PathLike, None] = None) -> Union[dict, None]:
        """Dump the request cache to dict or the specified filepath.

        Dump the request, response pairs stored in the `NdbcApi`'s
        `Request_handler` as a `dict`, either returning the object, if no
        `dest_fp` is specified, or serializing (pickling) the object and writing
        it to the specified `dest_fp`.

        Args:
            dest_fp: The destination filepath for the serialized `RequestsCache`
                contents.

        Returns:
            The cached request, response pairs as a `dict`, or `None` if a
            `dest_fp` is specified when calling the method.
        """
        return _dump_handler_cache_impl(
            getattr(self, '_handler', None),
            dest_fp=dest_fp,
        )

    def load_cache(self, src: Union[dict, str, os.PathLike]) -> None:
        """Load cached request/response pairs from a `dict` or pickle filepath.

        Counterpart to `dump_cache`. Populates the per-station LRU caches
        so subsequent offline or repeated queries hit the cache without
        network I/O.

        Args:
            src: A `dict` mapping `station_id` to `{url: response_dict}` as
                returned by `dump_cache`, or a filepath (`str` or `Path`)
                to a pickled cache dictionary.

        Raises:
            ValueError: If `src` is not a `dict` or a pickle file containing
                a `dict`.
        """
        _load_handler_cache_impl(getattr(self, '_handler', None), src=src)

    def clear_cache(self) -> None:
        """Clear the request cache and reset handler state."""
        if getattr(self, '_handler', None) is not None:
            with self._handler._lock:
                self._handler.stations.clear()
        self._handler = self._get_request_handler(
            cache_limit=self.cache_limit,
            delay=getattr(self, '_delay', HTTP_DELAY),
            retries=getattr(self, '_retries', HTTP_RETRY),
            backoff_factor=getattr(self, '_backoff_factor', HTTP_BACKOFF_FACTOR),
            headers=self.headers,
            debug=getattr(self, '_debug', HTTP_DEBUG),
            verify_https=getattr(self, '_verify_https', VERIFY_HTTPS),
        )
        self._handler.set_cache_limit(self.cache_limit)

    def set_cache_limit(self, new_limit: int) -> None:
        """Set the cache limit for the API's request cache."""
        self.cache_limit = new_limit
        self._handler.set_cache_limit(cache_limit=new_limit)

    def get_cache_limit(self) -> int:
        """Get the cache limit for the API's request cache."""
        return self._handler.get_cache_limit()

    def get_headers(self) -> dict:
        """Return the current headers used by the request handler."""
        return self._handler.get_headers()

    def update_headers(self, new: dict) -> None:
        """Add new headers to the request handler."""
        self.headers.update(new)
        self._handler.update_headers(new)

    def set_headers(self, request_headers: dict) -> None:
        """Reset the request headers using the new supplied headers."""
        self.headers = request_headers
        self._handler.set_headers(request_headers)

    def configure_logging(self, level=logging.WARNING, filename=None) -> None:
        """Configures logging for the NdbcApi.

        Args:
            level (int, optional): The logging level. Defaults to logging.WARNING.
            filename (str, optional): If provided, logs to the specified file.
        """
        self.logger.setLevel(level)

        handler: logging.Handler
        formatter: logging.Formatter

        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            if isinstance(handler, logging.FileHandler):
                handler.close()

        if filename:
            handler = logging.FileHandler(filename)
            formatter = logging.Formatter(
                '[%(asctime)s][%(levelname)s]: %(message)s')
        else:
            handler = logging.StreamHandler()
            formatter = LogFormatter('[%(levelname)s]: %(message)s')

        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def log(self,
            level: int,
            station_id: Union[int, str, None] = None,
            mode: Union[str, None] = None,
            message: Union[str, None] = None,
            **extra_data) -> None:
        """Logs a structured message with metadata.

        Args:
            level (int): The logging level.
            station_id (str, optional): The NDBC station ID.
            mode (str, optional): The data mode.
            message (str, optional): The log message.
            **extra_data: Additional key-value pairs to include in the log.
        """
        log_data = {}
        if station_id:
            log_data['station_id'] = station_id
        if mode:
            log_data['mode'] = mode
        if message:
            log_data['message'] = message
        for k, v in extra_data.items():
            log_data[k] = v
        self.logger.log(level, log_data)

    def stations(self, as_df: bool = True, as_pl: bool = False) -> Any:
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
            ResponseException: An error occurred while retrieving and parsing
                responses from the NDBC data service.
        """
        try:
            data = self._stations_api.stations(handler=self._handler)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError, AttributeError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def historical_stations(self,
                            as_df: bool = True,
                            as_pl: bool = False) -> Any:
        """Get historical stations and station metadata from the NDBC.

        Query the NDBC data service for the historical data buoys
        (stations), both those maintained by the NDBC and those which are not. 
        Stations are returned by default as rows of a `pandas.DataFrame`, 
        alongside their historical data coverage, with one row per tuple of 
        (station, historical deployment).

        Args:
            as_df: Flag indicating whether to return current station data as a
                `pandas.DataFrame` if set to `True` or as a `dict` if `False`.
            as_pl: Flag indicating whether to return current station data as a
                `polars.DataFrame` if set to `True`.

        Returns:
            The current station data from the NDBC data service, either as a
            `pandas.DataFrame`, `polars.DataFrame`, or as a `dict`.

        Raises:
            ResponseException: An error occurred while retrieving and parsing
                responses from the NDBC data service.
        """
        try:
            data = self._stations_api.historical_stations(handler=self._handler)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError, AttributeError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def nearest_station(
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
        """Get nearest station(s) to the specified lat/lon with optional capability filters.

        Use the NDBC data service's current station data to determine the
        nearest station to the specified latitude and longitude (either as
        `float` or as DD.dd[N/S/E/W] strings), optionally filtering by sensor
        capabilities or station metadata.

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
        """
        if lat is None or lon is None:
            raise ValueError('lat and lon must be specified.')
        if k < 1:
            raise ValueError('k must be >= 1.')
        lat_f = StationsHandler.LAT_MAP(lat)
        lon_f = StationsHandler.LON_MAP(lon)
        df = self.stations(as_df=False, as_pl=False)
        return self._stations_api.nearest_station(
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

    def radial_search(
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
        """Get all stations within radius units of the specified lat/lon.

        Use the NDBC data service's current station data to determine the
        stations within radius of the specified latitude and longitude
        (passed either as `float` or as DD.dd[N/S/E/W] strings), optionally
        filtered by sensor capabilities or station metadata.

        Args:
            lat (float): The latitude of interest.
            lon (float): The longitude of interest.
            radius (float): The radius in the specified units to search within.
            units (str: 'nm', 'km', or 'mi'): The units of the radius.
            as_df: Flag indicating whether to return current station data as a
                `pandas.DataFrame` if set to `True` or as a `dict` if `False`.
            as_pl: Flag indicating whether to return current station data as a
                `polars.DataFrame` if set to `True`.
            met: Optional filter for `'Includes Meteorology'`.
            currents: Optional filter for `'Includes Currents'`.
            waterquality: Optional filter for `'Includes Water Quality'`.
            dart: Optional filter for `'DART Program'`.
            owner: Optional case-insensitive substring filter for `'Owner'`.
            program: Optional case-insensitive substring filter for `'Program'`.
            station_type: Optional case-insensitive substring filter for `'Type'`.
            limit: Optional maximum number of closest stations to return.

        Returns:
            A `pandas.DataFrame`, `polars.DataFrame`, or dict of the stations within the specified radius.
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
        df = self.stations(as_df=False, as_pl=False)
        stations_in_radius = self._stations_api.radial_search(
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

    def station_deployments(
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
            as_df: Flag indicating whether to return deployment data as a
                `pandas.DataFrame` if set to `True` or as a `list` of `dict`
                records if `False`.
            as_pl: Flag indicating whether to return deployment data as a
                `polars.DataFrame` if set to `True`.

        Returns:
            Deployment records containing `'Start Date'`, `'End Date'`,
            `'Anemometer Height'`, `'Hull Type'`, `'Lat'`, `'Lon'`, and
            `'Elevation'`.
        """
        records = self.historical_stations(as_df=False, as_pl=False)
        filtered = _filter_station_deployments_impl(
            records=records,
            station_id=station_id,
            start_time=start_time,
            end_time=end_time,
        )
        return self._handle_data(filtered, as_df=as_df, as_pl=as_pl, cols=None)

    def explain_station(self, station_id: Union[str, int]) -> Dict[str, Any]:
        """Return a unified dossier of metadata, realtime modes, and historical coverage for a station.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`).

        Returns:
            A `dict` containing `'station_id'`, `'metadata'`, `'realtime_modes'`,
            and `'historical_modes'`.
        """
        sid = self._parse_station_id(station_id)
        meta = self.station(station_id=sid, as_df=False, as_pl=False)
        rt_modes = self.available_realtime(station_id=sid, full_response=False)
        hist_modes = self.available_historical(
            station_id=sid,
            full_response=False,
            as_df=False,
            as_pl=False,
        )
        return {
            'station_id': sid,
            'metadata': meta,
            'realtime_modes': rt_modes,
            'historical_modes': hist_modes,
        }

    def station(self,
                station_id: Union[str, int],
                as_df: bool = False,
                as_pl: bool = False) -> Any:
        """Get metadata for the given station from the NDBC.

        The NDBC maintains some station-level metadata including status notes,
        location information, inclement weather warnings, and measurement notes.
        This method is used to request, handle, and parse the metadata for the
        given station from the station's NDBC webpage.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`) for the
                station of interest.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.

        Returns:
            The station metadata for the given station, either as a `dict`,
            `pandas.DataFrame`, or `polars.DataFrame`.

        Raises:
            ResponseException: An error occurred when requesting and parsing
                responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            data = self._stations_api.metadata(handler=self._handler,
                                               station_id=station_id)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def available_realtime(
        self,
        station_id: Union[str, int],
        full_response: bool = False,
        as_df: Optional[bool] = None,
        as_pl: bool = False,
    ) -> Any:
        """Get the available realtime modalities for a station.

        While most data buoy (station) measurements are available over
        multi-year time ranges, some measurements depreciate or become
        unavailable for substantial periods of time. This method queries the
        NDBC station webpage for those measurements, and their links, which are
        available or were available over the last 45 days.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`) for the
                station of interest.
            full_response: Whether to return the full response from the NDBC
                API, defaults to `False` and a list of modes from `get_modes()`
                is returned. If `True`, the full URL for each data mode is 
                included in the returned `dict` or `pandas.DataFrame`.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.

        Returns:
            The available realtime measurements for the specified station,
            alongside their NDBC data links, either as a `dict`, `pandas.DataFrame`,
            or `polars.DataFrame`.

        Raises:
            ResponseException: An error occurred when requesting and parsing
                responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            station_realtime = self._stations_api.realtime(
                handler=self._handler, station_id=station_id)
            full_data = {}
            if full_response:
                if as_df is None and not as_pl:
                    as_df = False
                full_data = self._handle_data(station_realtime,
                                              as_df=as_df,
                                              as_pl=as_pl,
                                              cols=None)
                return full_data
            else:
                full_data = self._handle_data(station_realtime,
                                              as_df=False,
                                              as_pl=False,
                                              cols=None)

            # Parse the modes from the full response
            return _extract_realtime_modes_impl(full_data, self.get_modes())
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def available_historical(self,
                             station_id: Union[str, int],
                             as_df: bool = False,
                             as_pl: bool = False,
                             full_response: bool = True) -> Any:
        """Get the available historical measurements for a station.

        This method queries the NDBC station webpage for historical, quality
        controlled measurements and their associated availability time ranges.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`) for the
                station of interest.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.
            full_response: If `True` (default), return the full nested dictionary of
                historical URLs. If `False`, return a dictionary mapping canonical
                `ndbc-api` mode names to their available historical periods.

        Returns:
            The available historical measurements for the specified station,
            alongside their NDBC data links, either as a `dict`, `pandas.DataFrame`,
            or `polars.DataFrame`.

        Raises:
            ResponseException: An error occurred when requesting and parsing
                responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            data = self._stations_api.historical(handler=self._handler,
                                                 station_id=station_id)
            if not full_response:
                summary = _summarize_historical_modes_impl(data, self.get_modes())
                if not as_df and not as_pl:
                    return summary
                return self._handle_data(summary, as_df=as_df, as_pl=as_pl, cols=None)
            return self._handle_data(data, as_df=as_df, as_pl=as_pl, cols=None)
        except (ResponseException, ValueError, KeyError) as e:  # pragma: no cover
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
        return _build_requests_impl(
            station_id=station_id,
            mode=mode,
            start_time=start_time,
            end_time=end_time,
            use_opendap=use_opendap,
            as_xarray_dataset=as_xarray_dataset,
            station_ids=station_ids,
            modes=modes,
        )

    def get_data(
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
        """Execute data query against the specified NDBC station(s).

        Query the NDBC data service for station-level measurements, using the
        `mode` parameter to determine the measurement type (e.g. `'stdmet'` for
        standard meterological data or `'cwind'` for continuous winds data). The
        time range and data columns of interest may also be specified, such that
        a tailored set of requests are executed against the NDBC data service to
        generate a single `pandas.DataFrame` or `dict` matching the conditions
        specified in the method call. When calling `get_data` with `station_ids`
        the station identifier is added as a column to the returned data.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`) for the
                station of interest. For HF radar data, this should be an
                identifier such as `'uswc_6km'` for the US West Coast at 6 km
                resolution.
            station_ids: A list of NDBC station IDs (e.g. `['tplm2', '41001']`)
                for the stations of interest. Not supported for HF radar.
            mode: The data measurement type to query for the station (e.g.
                `'stdmet'` for standard meterological data or `'cwind'` for
                continuous winds data, or `'hfradar'` for HF radar data).
            modes: A list of data measurement types to query for the stations
                (e.g. `['stdmet', 'cwind']`, `'hfradar'` is not supported).
            start_time: The first timestamp of interest  (in UTC) for the data
                query, defaulting to 30 days before the current system time.
            end_time: The last timestamp of interest (in UTC) for the data
                query, defaulting to the current system time.
            use_timestamp: A flag indicating whether to parse the NDBC data
                service column headers as a timestamp, and to use this timestamp
                as the index.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `True`, if `False` a `dict` is returned unless
                `as_xarray_dataset` is set to `True`.
            as_pl: Whether to return station-level data as a `polars.DataFrame`,
                defaults to `False`.
            as_xarray_dataset: Whether to return tbe data as an `xarray.Dataset`,
                defaults to `False`.
            cols: A list of columns of interest which are selected from the 
                available data columns, such that only the desired columns are
                returned. All columns are returned if `None` is specified.
            use_opendap: An alias for `as_xarray_dataset`.

        Returns:
            The available station(s) measurements for the specified modes, time
            range, and columns, either as a `dict`, `pandas.DataFrame`, or
            `polars.DataFrame`.

        Raises:
            ValueError: Both `station_id` and `station_ids` are `None`, or both
                are not `None`. This is also raised if `mode` and `modes` are
                `None`, or both are not `None`
            RequestException: The specified mode is not available.
            ResponseException: There was an error in executing and parsing the
                required requests against the NDBC data service.
            HandlerException: There was an error in handling the returned data
                as a `dict` or `pandas.DataFrame`.
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
            raise ValueError('`station_id` and `station_ids` cannot both be '
                             'specified.')
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
            raise ValueError('`mode` and `modes` cannot both be specified.')

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

        # accumulated_data records the handled response and parsed station_id
        # as a tuple, with the data as the first value and the id as the second.
        accumulated_data: Dict[str, List[Any]] = {}
        for m in handle_modes:
            accumulated_data[m] = []
            if not handle_station_ids:
                continue

            with ThreadPoolExecutor(
                    max_workers=min(32, max(1, len(handle_station_ids)))) as station_executor:
                future_to_station = {
                    station_executor.submit(
                        self._handle_get_data,
                        mode=m,
                        station_id=sid,
                        start_time=start_dt,
                        end_time=end_dt,
                        use_timestamp=use_timestamp,
                        as_df=as_df,
                        as_pl=as_pl,
                        cols=cols,
                        use_opendap=as_xarray_dataset,
                    ): sid
                    for sid in handle_station_ids
                }

                for future in as_completed(future_to_station):
                    req_sid = future_to_station[future]
                    try:
                        station_data, parsed_sid = future.result()
                        self.log(
                            level=logging.DEBUG,
                            station_id=parsed_sid,
                            message=(
                                f'Successfully processed request for station_id {parsed_sid}'
                            ),
                        )
                        if not as_xarray_dataset:
                            # station_data is a list of dicts
                            for row in station_data:
                                row['station_id'] = parsed_sid
                            accumulated_data[m].append(station_data)
                        elif (
                            xarray is not None
                            and isinstance(station_data, xarray.Dataset)
                        ):
                            if len(handle_station_ids) > 1:
                                if not station_data.data_vars and not station_data.coords:
                                    continue
                                station_data = station_data.assign_coords(
                                    station_id=parsed_sid
                                )
                            accumulated_data[m].append(station_data)
                        else:
                            accumulated_data[m].append(station_data)
                    except (RequestException, ResponseException,
                            HandlerException) as e:  # pragma: no cover
                        self.log(
                            level=logging.WARN,
                            station_id=req_sid,
                            message=(
                                f'Failed to process request for station_id '
                                f'{req_sid} with error: {e}'))
        self.log(logging.INFO, message='Finished processing request.')
        return self._handle_accumulate_data(
            accumulated_data,
            as_df=as_df,
            as_pl=as_pl,
            as_xarray_dataset=as_xarray_dataset,
        )

    def get_modes(self,
                  use_opendap: bool = False,
                  as_xarray_dataset: Optional[bool] = None) -> List[str]:
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
            return list(OPENDAP_DISPATCH.keys())
        return list(HTTP_DISPATCH.keys())

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
        """
        Saves an `xarray.Dataset` to netCDF a user-specified file path.

        Args:
            dataset: The xarray dataset to save.
            output_filepath: The path to save the dataset to.
            **kwargs: Additional keyword arguments to pass to `dataset.to_netcdf`.

        Returns:
            None: The dataset is written to disk
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

    def _reconfigure(
        self,
        logging_level: Optional[int] = None,
        filename: Any = None,
        cache_limit: Optional[int] = None,
        headers: Optional[dict] = None,
        delay: Optional[int] = None,
        retries: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        verify_https: Optional[bool] = None,
        debug: Optional[bool] = None,
    ) -> None:
        """Apply explicit configuration arguments to an already-instantiated singleton."""
        if cache_limit is not None:
            self.set_cache_limit(cache_limit)
        if headers is not None:
            self.set_headers(headers)
        if delay is not None:
            self._delay = delay
            if self._handler is not None:
                self._handler._delay = delay
        if retries is not None:
            self._retries = retries
        if backoff_factor is not None:
            self._backoff_factor = backoff_factor
        if verify_https is not None:
            self._verify_https = verify_https
        if debug is not None:
            self._debug = debug
        if self._handler is not None:
            self._handler._reconfigure(
                cache_limit=cache_limit,
                log=self.log,
                delay=delay,
                retries=retries,
                backoff_factor=backoff_factor,
                headers=headers,
                debug=debug,
                verify_https=verify_https,
            )
        if logging_level is not None or filename is not None:
            lvl = logging_level if logging_level is not None else self.logger.level
            self.configure_logging(level=lvl, filename=filename)

    def _get_request_handler(
        self,
        cache_limit: int,
        delay: int,
        retries: int,
        backoff_factor: float,
        headers: dict,
        debug: bool,
        verify_https: bool,
    ) -> Any:
        """Build a new `RequestHandler` for the `NdbcApi`."""
        return RequestHandler(
            cache_limit=cache_limit or self.cache_limit,
            log=self.log,
            delay=delay,
            retries=retries,
            backoff_factor=backoff_factor,
            headers=headers,
            debug=debug,
            verify_https=verify_https,
        )

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
        """
        Accumulate the data from multiple stations and modes, coalescing
        overlapping data.
        """
        return _handle_accumulate_data_impl(
            accumulated_data,
            as_df=as_df,
            as_pl=as_pl,
            as_xarray_dataset=as_xarray_dataset,
        )

    def _handle_get_data(
        self,
        mode: str,
        station_id: str,
        start_time: datetime,
        end_time: datetime,
        use_timestamp: bool,
        as_df: bool = True,
        as_pl: bool = False,
        cols: List[str] = None,
        use_opendap: bool = False,
    ) -> Tuple[Any, str]:
        start_time = self._handle_timestamp(start_time)
        end_time = self._handle_timestamp(end_time)
        if start_time > end_time:
            raise TimestampException(
                f'`start_time` ({start_time}) must be <= `end_time` ({end_time}).'
            )
        station_id = self._parse_station_id(station_id)
        if use_opendap:
            data_api_call = getattr(self._opendap_data_api, mode, None)  # pragma: no cover
        else:
            data_api_call = getattr(self._data_api, mode, None)
        if not data_api_call:  # pragma: no cover
            raise RequestException(
                'Please supply a supported mode from `get_modes()`.')
        try:
            data = data_api_call(
                self._handler,
                station_id,
                start_time,
                end_time,
                use_timestamp,
            )
        except (ResponseException, ValueError, TypeError, KeyError) as e:  # pragma: no cover
            raise ResponseException(
                f'Failed to handle API call.\nRaised from {e}') from e
        if use_timestamp:
            if use_opendap:  # pragma: no cover
                data = filter_dataset_by_time_range(data, start_time, end_time)
            else:
                data = self._enforce_timerange(df=data,
                                               start_time=start_time,
                                               end_time=end_time)
        try:
            if use_opendap:  # pragma: no cover
                if cols:
                    handled_data = filter_dataset_by_variable(data, cols)
                else:
                    handled_data = data
            else:
                # Keep it as raw list of dicts during accumulation to make merging easy!
                handled_data = self._handle_data(data, as_df=False, as_pl=False, cols=cols)
        except (ValueError, KeyError, AttributeError) as e:  # pragma: no cover
            raise ParserException(
                f'Failed to handle returned data.\nRaised from {e}') from e

        return (handled_data, station_id)
