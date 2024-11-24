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
import pickle
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, List, Sequence, Tuple, Union, Dict

import xarray
import pandas as pd

from .api.handlers.http.data import DataHandler
from .api.handlers.http.stations import StationsHandler
from .config import (DEFAULT_CACHE_LIMIT, HTTP_BACKOFF_FACTOR, HTTP_DEBUG,
                     HTTP_DELAY, HTTP_RETRY, LOGGER_NAME, VERIFY_HTTPS)
from .exceptions import (HandlerException, ParserException, RequestException,
                         ResponseException, TimestampException)
from .utilities.req_handler import RequestHandler
from .utilities.singleton import Singleton
from .utilities.log_formatter import LogFormatter
from .api.handlers.opendap.data import OpenDapDataHandler
from .utilities.opendap.dataset import concat_datasets, merge_datasets, filter_dataset_by_variable, filter_dataset_by_time_range


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
    warnings.simplefilter(action='ignore', category=FutureWarning)

    def __init__(
        self,
        logging_level: int = logging.WARNING if HTTP_DEBUG else logging.ERROR,
        filename: Any = None,
        cache_limit: int = DEFAULT_CACHE_LIMIT,
        headers: dict = {},
        delay: int = HTTP_DELAY,
        retries: int = HTTP_RETRY,
        backoff_factor: float = HTTP_BACKOFF_FACTOR,
        verify_https: bool = VERIFY_HTTPS,
        debug: bool = HTTP_DEBUG,
    ):
        """Initializes the singleton `NdbcApi`, sets associated handlers."""
        self.cache_limit = cache_limit
        self.headers = headers
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

    def dump_cache(self, dest_fp: Union[str, None] = None) -> Union[dict, None]:
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
        data = dict()
        ids = [r.id_ for r in self._handler.stations]
        caches = [r.reqs.cache for r in self._handler.stations]
        if ids:
            for station_id, cache in zip(ids, caches):
                data[station_id] = dict()
                reqs = cache.keys()
                for req in reqs:
                    resp = cache[req].v
                    data[station_id][req] = resp
        if dest_fp:
            with open(dest_fp, 'wb') as f:
                pickle.dump(data, f)
        else:
            return data

    def clear_cache(self) -> None:
        """Clear the request cache and create a new handler."""
        del self._handler
        self._handler = self._get_request_handler(
            cache_limit=self.cache_limit,
            delay=HTTP_DELAY,
            retries=HTTP_RETRY,
            backoff_factor=HTTP_BACKOFF_FACTOR,
            headers=self.headers,
            debug=HTTP_DEBUG,
            verify_https=VERIFY_HTTPS,
        )

    def set_cache_limit(self, new_limit: int) -> None:
        """Set the cache limit for the API's request cache."""
        self._handler.set_cache_limit(cache_limit=new_limit)

    def get_cache_limit(self) -> int:
        """Get the cache limit for the API's request cache."""
        return self._handler.get_cache_limit()

    def get_headers(self) -> dict:
        """Return the current headers used by the request handler."""
        return self._handler.get_headers()

    def update_headers(self, new: dict) -> None:
        """Add new headers to the request handler."""
        self._handler.update_headers(new)

    def set_headers(self, request_headers: dict) -> None:
        """Reset the request headers using the new supplied headers."""
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

    def stations(self, as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get all stations and station metadata from the NDBC.

        Query the NDBC data service for the current available data buoys
        (stations), both those maintained by the NDBC and those whose
        measurements are managed by the NDBC. Stations are returned by default
        as rows of a `pandas.DataFrame`, alongside their realtime data coverage
        for some common measurements, their latitude and longitude, and current
        station status notes maintained by the NDBC.

        Args:
            as_df: Flag indicating whether to return current station data as a
                `pandas.DataFrame` if set to `True` or as a `dict` if `False`.

        Returns:
            The current station data from the NDBC data service, either as a
            `pandas.DataFrame` or as a `dict` depending on the value of `as_df`.

        Raises:
            ResponseException: An error occurred while retrieving and parsing
                responses from the NDBC data service.
        """
        try:
            data = self._stations_api.stations(handler=self._handler)
            return self._handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def historical_stations(self,
                            as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get historical stations and station metadata from the NDBC.

        Query the NDBC data service for the historical data buoys
        (stations), both those maintained by the NDBC and those which are not. 
        Stations are returned by default as rows of a `pandas.DataFrame`, 
        alongside their historical data coverage, with one row per tuple of 
        (station, historical deployment).

        Args:
            as_df: Flag indicating whether to return current station data as a
                `pandas.DataFrame` if set to `True` or as a `dict` if `False`.

        Returns:
            The current station data from the NDBC data service, either as a
            `pandas.DataFrame` or as a `dict` depending on the value of `as_df`.

        Raises:
            ResponseException: An error occurred while retrieving and parsing
                responses from the NDBC data service.
        """
        try:
            data = self._stations_api.historical_stations(handler=self._handler)
            return self._handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def nearest_station(
        self,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
    ) -> str:
        """Get nearest station to the specified lat/lon.

        Use the NDBC data service's current station data to determine the
        nearest station to the specified latitude and longitude (either as
        `float` or as DD.dd[E/W] strings).

        Args:
            lat: The latitude of interest, used to determine the closest
                maintained station to the given position.
            lon: The longitude of interest, used to determine the closest
                maintained station to the given position.

        Returns:
            The station id (e.g. `'tplm2'` or `'41001'`) of the nearest station
            with active measurements to the specified lat/lon pair.

        Raises:
            ValueError: The latitude and longitude were not both specified when
                querying for the closest station.
        """
        if not (lat and lon):
            raise ValueError('lat and lon must be specified.')
        nearest_station = self._stations_api.nearest_station(
            handler=self._handler, lat=lat, lon=lon)
        return nearest_station

    def radial_search(
        self,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
        radius: float = -1,
        units: str = 'km',
    ) -> pd.DataFrame:
        """Get all stations within radius units of the specified lat/lon.

        Use the NDBC data service's current station data to determine the
        stations within radius of the specified latitude and longitude
        (passed either as `float` or as DD.dd[E/W] strings).

        Args:
            lat (float): The latitude of interest, used to determine the maintained
                stations within radius units of the given position.
            lon (float): The longitude of interest, used to determine the maintained
                stations within radius units of the given position.
            radius (float): The radius in the specified units to search for stations
                within.
            units (str: 'nm', 'km', or 'mi'): The units of the radius, either 'nm', 'km', or 'mi'.

        Returns:
            A `pandas.DataFrame` of the stations within the specified radius of
            the given lat/lon pair.

        Raises:
            ValueError: The latitude and longitude were not both specified when
                querying for the closest station, or the radius or units are
                invalid.
        """
        if not (lat and lon):
            raise ValueError('lat and lon must be specified.')
        stations_in_radius = self._stations_api.radial_search(
            handler=self._handler, lat=lat, lon=lon, radius=radius, units=units)
        return stations_in_radius

    def station(self,
                station_id: Union[str, int],
                as_df: bool = False) -> Union[pd.DataFrame, dict]:
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

        Returns:
            The station metadata for the given station, either as a `dict` or as
            a `pandas.DataFrame` if the `as_df` flag is set to `True`.

        Raises:
            ResponseException: An error occurred when requesting and parsing
                responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            data = self._stations_api.metadata(handler=self._handler,
                                               station_id=station_id)
            return self._handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def available_realtime(self,
                           station_id: Union[str, int],
                           as_df: bool = False) -> Union[pd.DataFrame, dict]:
        """Get the available realtime measurements for a station.

        While most data buoy (station) measurements are available over
        multi-year time ranges, some measurements depreciate or become
        unavailable for substantial periods of time. This method queries the
        NDBC station webpage for those measurements, and their links, which are
        available or were available over the last 45 days.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`) for the
                station of interest.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.

        Returns:
            The available realtime measurements for the specified station,
            alongside their NDBC data links, either as a `dict` or as a
            `pandas.DataFrame` if the `as_df` flag is set to `True`.

        Raises:
            ResponseException: An error occurred when requesting and parsing
                responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            data = self._stations_api.realtime(handler=self._handler,
                                               station_id=station_id)
            return self._handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def available_historical(self,
                             station_id: Union[str, int],
                             as_df: bool = False) -> Union[pd.DataFrame, dict]:
        """Get the available historical measurements for a station.

        This method queries the NDBC station webpage for historical, quality
        controlled measurements and their associated availability time ranges.

        Args:
            station_id: The NDBC station ID (e.g. `'tplm2'` or `41001`) for the
                station of interest.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.

        Returns:
            The available historical measurements for the specified station,
            alongside their NDBC data links, either as a `dict` or as a
            `pandas.DataFrame` if the `as_df` flag is set to `True`.

        Raises:
            ResponseException: An error occurred when requesting and parsing
                responses for the specified station.
        """
        station_id = self._parse_station_id(station_id)
        try:
            data = self._stations_api.historical(handler=self._handler,
                                                 station_id=station_id)
            return self._handle_data(data, as_df, cols=None)
        except (ResponseException, ValueError, KeyError) as e:
            raise ResponseException('Failed to handle returned data.') from e

    def get_data(
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
        use_opendap: bool = False,
    ) -> Union[pd.DataFrame, xarray.Dataset, dict]:
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
                station of interest.
            station_ids: A list of NDBC station IDs (e.g. `['tplm2', '41001']`)
                for the stations of interest.
            mode: The data measurement type to query for the station (e.g.
                `'stdmet'` for standard meterological data or `'cwind'` for
                continuous winds data).
            modes: A list of data measurement types to query for the stations
                (e.g. `['stdmet', 'cwind']`).
            start_time: The first timestamp of interest  (in UTC) for the data
                query, defaulting to 30 days before the current system time.
            end_time: The last timestamp of interest (in UTC) for the data
                query, defaulting to the current system time.
            use_timestamp: A flag indicating whether to parse the NDBC data
                service column headers as a timestamp, and to use this timestamp
                as the index.
            as_df: Whether to return station-level data as a `pandas.DataFrame`,
                defaults to `False`, and a `dict` is returned.
            cols: A list of columns of interest which are selected from the 
                available data columns, such that only the desired columns are
                returned. All columns are returned if `None` is specified.

        Returns:
            The available station(s) measurements for the specified modes, time
            range, and columns, either as a `dict` or as a `pandas.DataFrame`
            if the `as_df` flag is set to `True`.

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
        self.log(logging.DEBUG,
                 message=f"`get_data` called with arguments: {locals()}")
        if station_id is None and station_ids is None:
            raise ValueError('Both `station_id` and `station_ids` are `None`.')
        if station_id is not None and station_ids is not None:
            raise ValueError('`station_id` and `station_ids` cannot both be '
                             'specified.')
        if mode is None and modes is None:
            raise ValueError('Both `mode` and `modes` are `None`.')
        if mode is not None and modes is not None:
            raise ValueError('`mode` and `modes` cannot both be specified.')

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

        for mode in handle_modes:
            if mode not in self.get_modes(use_opendap):
                raise RequestException(f"Mode {mode} is not available.")

        self.log(logging.INFO,
                 message=(f"Processing request for station_ids "
                          f"{handle_station_ids} and modes "
                          f"{handle_modes}"))

        # accumulated_data records the handled response and parsed station_id
        # as a tuple, with the data as the first value and the id as the second.
        accumulated_data: Dict[str, Dict[str, Union[pd.DataFrame, dict]]] = {}
        for mode in handle_modes:
            accumulated_data[mode] = []

            with ThreadPoolExecutor(
                    max_workers=len(handle_station_ids)) as station_executor:
                station_futures = {}
                for station_id in handle_station_ids:
                    station_futures[station_id] = station_executor.submit(
                        self._handle_get_data,
                        mode=mode,
                        station_id=station_id,
                        start_time=start_time,
                        end_time=end_time,
                        use_timestamp=use_timestamp,
                        as_df=as_df,
                        cols=cols,
                        use_opendap=use_opendap,
                    )

                for future in as_completed(station_futures.values()):
                    try:
                        station_data, station_id = future.result()
                        self.log(
                            level=logging.DEBUG,
                            station_id=station_id,
                            message=
                            f"Successfully processed request for station_id {station_id}"
                        )
                        if as_df:
                            station_data['station_id'] = station_id
                        accumulated_data[mode].append(station_data)
                    except (RequestException, ResponseException,
                            HandlerException) as e:
                        self.log(
                            level=logging.WARN,
                            station_id=station_id,
                            message=(f"Failed to process request for station_id "
                                    f"{station_id} with error: {e}"))
        self.log(logging.INFO, message="Finished processing request.")
        return self._handle_accumulate_data(accumulated_data)

            
    def get_modes(self, use_opendap: bool = False) -> List[str]:
        """Get the list of supported modes for `get_data(...)`.
        
        Args:
            use_opendap (bool): Whether to return the available
                modes for opendap (NetCDF) data.
        
        Returns:
            (List[str]) the available modalities.
        """
        if use_opendap:
            return [
                v for v in vars(self._opendap_data_api) if not v.startswith('_')
            ]
        return [v for v in vars(self._data_api) if not v.startswith('_')]

    @staticmethod
    def save_netcdf_dataset(dataset: xarray.Dataset, output_filepath: str):
        """
        Saves a netCDF4 dataset from a temporary file to a user-specified file path.

        Args:
            dataset: The xarray dataset to save.
            output_filepath: The path to save the dataset to.
        """
        dataset.to_netcdf(output_filepath)
        
    """ PRIVATE """

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
        station_id = str(station_id)  # expect string-valued station id
        station_id = station_id.lower()  # expect lowercased station id
        return station_id

    @staticmethod
    def _handle_timestamp(timestamp: Union[datetime, str]) -> datetime:
        """Convert the specified timestamp to `datetime.datetime`."""
        if isinstance(timestamp, datetime):
            return timestamp
        else:
            try:
                return datetime.strptime(timestamp, '%Y-%m-%d')
            except ValueError as e:
                raise TimestampException from e

    @staticmethod
    def _enforce_timerange(df: pd.DataFrame, start_time: datetime,
                           end_time: datetime) -> pd.DataFrame:
        """Down-select to the data within the specified `datetime` range."""
        try:
            df = df.loc[(df.index.values >= pd.Timestamp(start_time)) &
                        (df.index.values <= pd.Timestamp(end_time))]
        except ValueError as e:
            raise TimestampException(
                'Failed to enforce `start_time` to `end_time` range.') from e
        return df

    @staticmethod
    def _handle_data(data: pd.DataFrame,
                     as_df: bool = True,
                     cols: List[str] = None) -> Union[pd.DataFrame, dict]:
        """Apply column down selection and return format handling."""
        if cols:
            try:
                data = data[[*cols]]
            except (KeyError, ValueError) as e:
                raise ParserException(
                    'Failed to parse column selection.') from e
        if as_df and isinstance(data, pd.DataFrame):
            return data
        elif isinstance(data, pd.DataFrame) and not as_df:
            return data.to_dict()
        elif as_df:
            try:
                return pd.DataFrame().from_dict(data, orient='index')
            except (NotImplementedError, ValueError, TypeError) as e:
                raise HandlerException(
                    'Failed to convert `pd.DataFrame` to `dict`.') from e
        else:
            return data

    def _handle_accumulate_data(
        self,
        accumulated_data: Dict[str, List[Union[pd.DataFrame, dict, xarray.Dataset]]],
    ) -> Union[pd.DataFrame, dict]:
        """Accumulate the data from multiple stations and modes."""
        for k in list(accumulated_data.keys()):
            if not accumulated_data[k]:
                del accumulated_data[k]
        
        if not accumulated_data:
            return {}
        
        return_as_df = isinstance(accumulated_data[list(accumulated_data.keys())[-1]][0], pd.DataFrame)
        use_opendap = isinstance(accumulated_data[list(accumulated_data.keys())[-1]][0], xarray.Dataset)

        data: Union[List[pd.DataFrame], List[xarray.Dataset],
                    dict] = [] if return_as_df or use_opendap else {}

        for mode, station_data in accumulated_data.items():
            if return_as_df:
                data.extend(station_data)
            elif use_opendap:
                data.extend(station_data)
            else:
                data[mode] = station_data

        if return_as_df:
            df = pd.concat(data, axis=0)
            df.reset_index(inplace=True, drop=False)
            df.set_index(['timestamp', 'station_id'], inplace=True)
            return df
        elif use_opendap:
            return merge_datasets(data)
        return data

    def _handle_get_data(
        self,
        mode: str,
        station_id: str,
        start_time: datetime,
        end_time: datetime,
        use_timestamp: bool,
        as_df: bool = True,
        cols: List[str] = None,
        use_opendap: bool = False,
    ) -> Tuple[Union[pd.DataFrame, xarray.Dataset, dict], str]:
        start_time = self._handle_timestamp(start_time)
        end_time = self._handle_timestamp(end_time)
        station_id = self._parse_station_id(station_id)
        if use_opendap:
            data_api_call = getattr(self._opendap_data_api, mode, None)
        else:
            data_api_call = getattr(self._data_api, mode, None)
        if not data_api_call:
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
        except (ResponseException, ValueError, TypeError, KeyError) as e:
            raise ResponseException(
                f'Failed to handle API call.\nRaised from {e}') from e
        if use_timestamp:
            if use_opendap:
                data = filter_dataset_by_time_range(data, start_time, end_time)
            else:
                data = self._enforce_timerange(df=data,
                                               start_time=start_time,
                                               end_time=end_time)
        try:
            if use_opendap:
                if cols:
                    handled_data = filter_dataset_by_variable(data, cols)
                else:
                    handled_data = data
            else:
                handled_data = self._handle_data(data, as_df, cols)
        except (ValueError, KeyError, AttributeError) as e:
            raise ParserException(
                f'Failed to handle returned data.\nRaised from {e}') from e

        return (handled_data, station_id)
