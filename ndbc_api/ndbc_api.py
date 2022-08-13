import logging
import pickle
from datetime import datetime, timedelta
from typing import Any, Union, List

import pandas as pd

from .utilities.singleton import Singleton
from .utilities.req_handler import RequestHandler
from .api.handlers.stations import StaitonsHandler
from .api.handlers.data import DataHandler
from .config import (
    LOGGER_NAME,
    DEFAULT_CACHE_LIMIT,
    HTTP_DEBUG,
    HTTP_DELAY,
    VERIFY_HTTPS,
    HTTP_BACKOFF_FACTOR,
    HTTP_RETRY,
)


class NdbcApi(metaclass=Singleton):

    log = logging.getLogger(LOGGER_NAME)

    def __init__(
        self,
        logging_level: int = logging.WARNING if HTTP_DEBUG else 0,
        cache_limit: int = DEFAULT_CACHE_LIMIT,
        delay: int = HTTP_DELAY,
        retries: int = HTTP_RETRY,
        backoff_factor: float = HTTP_BACKOFF_FACTOR,
        verify_https: bool = VERIFY_HTTPS,
        debug: bool = HTTP_DEBUG,
    ):
        self.log.setLevel(logging_level)
        self.cache_limit = cache_limit
        self._handler = self._get_request_handler(
            cache_limit=self.cache_limit,
            delay=delay,
            retries=retries,
            backoff_factor=backoff_factor,
            debug=debug,
            verify_https=verify_https,
        )
        self._stations_api = StaitonsHandler
        self._data_api = DataHandler

    def dump_cache(self, dest_fp: str = None) -> None:
        """Dump the request cache to dict, or optionally to disk using the specified filepath."""
        data = dict()
        ids = [r.id_ for r in self._handler.stations]
        caches = [r.reqs.cache for r in self._handler.stations]
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
        """Clear the request cache."""
        self._handler = self._get_request_handler(
            cache_limit=self._cache_limit,
            delay=HTTP_DELAY,
            retries=HTTP_RETRY,
            backoff_factor=HTTP_BACKOFF_FACTOR,
            debug=HTTP_DEBUG,
            verify_https=VERIFY_HTTPS,
        )

    def update_cache_limit(self, new_limit: int) -> None:
        """Change the cache limit for the API's request cache."""
        self._handler.update_cache_limit(cache_limit=new_limit)

    def stations(self, as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        return self._stations_api.stations(handler=self._handler, as_df=as_df)

    def nearest_station(
        self,
        lat: Union[str, float] = None,
        lon: Union[str, float] = None,
        as_df: bool = False,
    ) -> str:
        """Get nearest station."""
        if not (lat and lon):
            raise ValueError('lat and lon must be specified.')
        return self._stations_api.nearest_station(
            handler=self._handler, lat=lat, lon=lon, as_df=as_df
        )

    def station(
        self, station_id: Union[str, int], as_df: bool = False
    ) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        station_id = self._parse_station_id(station_id)
        return self._stations_api.metadata(
            handler=self._handler, station_id=station_id, as_df=as_df
        )

    def available_realtime(
        self, station_id: Union[str, int], as_df: bool = False
    ) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        station_id = self._parse_station_id(station_id)
        return self._stations_api.realtime(
            handler=self._handler, station_id=station_id, as_df=as_df
        )

    def available_historical(
        self, station_id: Union[str, int], as_df: bool = False
    ) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        station_id = self._parse_station_id(station_id)
        return self._stations_api.historical(
            handler=self._handler, station_id=station_id, as_df=as_df
        )

    def get_data(
        self,
        station_id: Union[int, str],
        mode: str,
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """Execute data query against a station."""
        if not isinstance(start_time, datetime):
            try:
                start_time = datetime.fromisoformat(start_time)
            except ValueError as e:
                raise ValueError(
                    'Please supply start time as ISO formatted string.'
                ) from e
        station_id = self._parse_station_id(station_id)
        data_api_call = getattr(self._data_api, mode)
        if not data_api_call:
            raise NotImplementedError(
                'Please supply a supported mode from `get_modes()`.'
            )
        try:
            return data_api_call(
                self._handler,
                station_id,
                cols,
                start_time,
                end_time,
                use_timestamp,
                as_df,
            )
        except Exception as e:
            raise ValueError('hit an error') from e

    def get_modes(self):
        """Get the list of supported modes."""
        return [v for v in vars(self._data_api) if not v.startswith('_')]

    """ PRIVATE """

    def _get_request_handler(
        self,
        cache_limit: int,
        delay: int,
        retries: int,
        backoff_factor: float,
        debug: bool,
        verify_https: bool,
    ) -> Any:
        return RequestHandler(
            cache_limit=cache_limit or self.cache_limit,
            log=self.log,
            delay=delay,
            retries=retries,
            backoff_factor=backoff_factor,
            debug=debug,
            verify_https=verify_https,
        )

    def _parse_station_id(self, station_id: Union[str, int]) -> str:
        """Parse station id"""
        station_id = str(station_id)  # expect string-valued station id
        station_id = station_id.lower()  # expect lowercased station id
        return station_id


if __name__ == '__main__':
    api = NdbcApi()
    df = api.get_data('44095', 'swdir', None, '2022-01-01')
    print(len(df))
