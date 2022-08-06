from typing import Any, Union

import pandas as pd

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.requests.stations import StationsRequest
from ndbc_api.api.requests.station_metadata import MetadataRequest
from ndbc_api.api.requests.station_historical import HistoricalRequest
from ndbc_api.api.requests.station_realtime import RealtimeRequest

from ndbc_api.api.parsers.stations import StationsParser
from ndbc_api.api.parsers.station_metadata import MetadataParser
from ndbc_api.api.parsers.station_historical import HistoricalParser
from ndbc_api.api.parsers.station_realtime import RealtimeParser


class StaitonsHandler(BaseHandler):

    @classmethod
    def stations(cls, handler: Any, as_df: bool) -> Union[pd.DataFrame, dict]:
        """Get all stations from NDBC."""
        req = StationsRequest.build_request()
        resp = handler.handle_request('stn', req)
        data = StationsParser.df_from_responses([resp])
        if as_df:
            return data
        else:
            return data.to_records()

    @classmethod
    def nearest_station(cls, handler: Any, lat: Union[str, float], lon: Union[str, float], as_df: bool) -> str:
        """Get nearest station."""
        pass

    @classmethod
    def metadata(cls, handler: Any, station_id: str, as_df: bool) -> Union[pd.DataFrame, dict]:
        """Get station description."""
        req = MetadataRequest.build_request(station_id=station_id)
        resp = handler.handle_request(station_id, req)
        data = MetadataParser.metadata(resp)
        if as_df:
            return pd.DataFrame(data)
        else:
            return data

    @classmethod
    def realtime(cls, handler: Any, station_id: str, as_df: bool) -> Union[pd.DataFrame, dict]:
        """Get the available realtime measurements for a station."""
        req = RealtimeRequest.build_request(station_id=station_id)
        resp = handler.handle_request(station_id, req)
        data = RealtimeParser.available_measurments(resp)
        if as_df:
            return pd.DataFrame(data)
        else:
            return data

    @classmethod
    def historical(cls, handler: Any, station_id: str, as_df: bool) -> Union[pd.DataFrame, dict]:
        """Get the available historical measurements for a station."""
        req = HistoricalRequest.build_request(station_id=station_id)
        resp = handler.handle_request(station_id, req)
        data = HistoricalParser.available_measurments(resp)
        if as_df:
            return pd.DataFrame(data)
        else:
            return data
