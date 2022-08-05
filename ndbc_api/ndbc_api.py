import logging
import pickle
from typing import Any, Union, List
from datetime import datetime, timedelta

import pandas as pd

from .utilities.req_handler import RequestHandler
from .api.requests.stations import StationsRequest
from .api.requests.adcp import AdcpRequest
from .api.requests.stdmet import StdmetRequest
from .api.parsers.stations import StationsParser
from .api.parsers.adcp import AdcpParser
from .api.parsers.stdmet import StdmetParser
from .config import LOGGER_NAME


DEFAULT_CACHE_LIMIT = 36


class NdbcApi:

    log = logging.getLogger(LOGGER_NAME)

    def __init__(
        self,
        logging_level: int = logging.WARNING,
        cache_limit: int = DEFAULT_CACHE_LIMIT,
        ):
        self.log.setLevel(logging_level)
        self.cache_limit = cache_limit
        self.handler = self._get_request_handler(cache_limit=cache_limit)
        self._handlers = self._register_handlers()

    def dump_cache(self, dest_fp: str) -> None:
        with open(dest_fp, 'wb') as f:
            pickle.dump(self.handler, f)

    def clear_cache(self) -> None:
        self.handler = self._get_request_handler(cache_limit=self.cache_limit)
    
    def update_cache_limit(self, new_limit: int) -> None:
        self.handler.update_cache_limit(cache_limit=new_limit)

    def stations(self, as_df: bool = True) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        req = StationsRequest.build_request()
        resp = self.handler.handle_request('stn', req)
        data = StationsParser.df_from_responses([resp])
        if as_df:
            return data
        else:
            return data.to_records()

    def nearest_station(self, lat, lon) -> str:
        """Get nearest station."""
        pass

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
        station_id = str(station_id)
        station_id = station_id.lower()
        return self._handlers[mode](
            station_id=station_id,
            start_time=start_time,
            end_time=end_time,
            cols=cols,
            use_timestamp=use_timestamp,
            as_df=as_df
        )

    
    def _get_request_handler(self, cache_limit: int) -> Any:
        return RequestHandler(cache_limit=cache_limit or self.cache_limit, log=self.log)
    
    def _register_handlers(self) -> dict:
        return {
            'adcp': self._adcp,
            'cwind': self._cwind,
            'data_spec': self._data_spec,
            'ocean': self._ocean,
            'spec': self._spec,
            'stdmet': self._stdmet,
            'supl': self._supl,
            'swden': self._swden,
            'swdir': self._swdir,
            'swdir2': self._swdir2,
            'swr1': self._swr1,
            'swr2': self._swr2,
        }

    def _adcp(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """adcp"""
        reqs = AdcpRequest.build_request(station_id=station_id, start_time=start_time, end_time=end_time)
        resps = self.handler.handle_requests(station_id, reqs)
        data = AdcpParser.df_from_responses(resps)
        if as_df:
            return data
        else:
            return data.to_records()

    def _cwind(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """cwind"""
        pass

    def _data_spec(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """data_spec"""
        pass

    def _ocean(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """ocean"""
        pass

    def _spec(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """spec"""
        pass

    def _stdmet(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """stdmet"""
        reqs = StdmetRequest.build_request(station_id=station_id, start_time=start_time, end_time=end_time)
        resps = self.handler.handle_requests(station_id, reqs)
        data = StdmetParser.df_from_responses(resps)
        if as_df:
            return data
        else:
            return data.to_records()

    def _supl(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """supl"""
        pass

    def _swden(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """swden"""
        pass

    def _swdir(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """swdir"""
        pass

    def _swdir2(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """swdir2"""
        pass

    def _swr1(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """swr1"""
        pass

    def _swr2(self, station_id, start_time, end_time, cols, use_timestamp, as_df) -> Union[pd.DataFrame, dict]:
        """swr2"""
        pass


if __name__ == '__main__':
    api = NdbcApi()
    vals = api.get_data(
        mode='stdmet',
        start_time=datetime.fromisoformat('2020-01-01'),
        end_time=datetime.fromisoformat('2022-05-20'),
        station_id='tplm2',
        cols=None,
    )
    print(len(vals))
