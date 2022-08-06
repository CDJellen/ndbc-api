import logging
import pickle
from typing import Any, Union, List
from datetime import datetime, timedelta

import pandas as pd

from .utilities.singleton import Singleton
from .utilities.req_handler import RequestHandler
from .api.handlers.stations import StaitonsHandler
from .config import LOGGER_NAME


DEFAULT_CACHE_LIMIT = 36


class NdbcApi(metaclass=Singleton):

    log = logging.getLogger(LOGGER_NAME)

    def __init__(
        self,
        logging_level: int = logging.WARNING,
        cache_limit: int = DEFAULT_CACHE_LIMIT,
        ):
        self.log.setLevel(logging_level)
        self.cache_limit = cache_limit
        self._handler = self._get_request_handler(cache_limit=cache_limit)
        self._stations_api = StaitonsHandler
        self._data_api = None  # TODO

    def dump_cache(self, dest_fp: str) -> None:
        with open(dest_fp, 'wb') as f:
            pickle.dump(self._handler, f)

    def clear_cache(self) -> None:
        self._handler = self._get_request_handler(cache_limit=self.cache_limit)

    def update_cache_limit(self, new_limit: int) -> None:
        self._handler.update_cache_limit(cache_limit=new_limit)

    def stations(self, as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        return self._stations_api.stations(handler=self._handler, as_df=as_df)

    def nearest_station(self, lat: Union[str, float] = None, lon: Union[str, float] = None, as_df: bool = False) -> str:
        """Get nearest station."""
        if not (lat and lon):
            raise ValueError('lat and lon must be specified.')
        return self._stations_api.nearest_station(handler=self._handler, lat=lat, lon=lon, as_df=as_df)

    def station(self, station_id: Union[str, int], as_df: bool = False) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        station_id = self._parse_station_id(station_id)
        return self._stations_api.metadata(handler=self._handler, station_id=station_id, as_df=as_df)

    def available_realtime(self, station_id: Union[str, int], as_df: bool = False) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        station_id = self._parse_station_id(station_id)
        return self._stations_api.realtime(handler=self._handler, station_id=station_id, as_df=as_df)

    def available_historical(self,  station_id: Union[str, int], as_df: bool = False) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        station_id = self._parse_station_id(station_id)
        return self._stations_api.historical(handler=self._handler, station_id=station_id, as_df=as_df)

    def get_data(
        self,
        station_id: Union[int, str],
        mode: str,
        cols: List[str],
        start_time: Union[str, datetime] = datetime.now()-timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True
        ) -> Union[pd.DataFrame, dict]:
        """Execute data query against a station."""
        if not isinstance(start_time, datetime):
            try:
                start_time = datetime.fromisoformat(start_time)
            except ValueError as e:
                raise ValueError('Please supply start time as ISO formatted string.') from e
        if mode not in self._handlers:
            raise NotImplementedError('Supported data requests are ' +
                ['\"'+r+'\"'+'\n' for r in self._handlers]
            )
        station_id = self._parse_station_id(station_id)
        return self._data_api


    """ PRIVATE """


    def _get_request_handler(self, cache_limit: int) -> Any:
        return RequestHandler(cache_limit=cache_limit or self.cache_limit, log=self.log)

    def _parse_station_id(self, station_id: Union[str, int]):
        """Parse station id"""
        station_id = str(station_id)  # expect string-valued station id
        station_id = station_id.lower()  # expect lowercased station id
        return station_id
