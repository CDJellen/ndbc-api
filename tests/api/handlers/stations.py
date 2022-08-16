from math import cos, asin, sqrt, pi
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

    DEG_TO_RAD = pi / 180
    DIAM_OF_EARTH = 12756
    LAT_MAP = (
        lambda x: -1 * float(x.strip('S')) if 'S' in x else float(x.strip('N'))
    )
    LON_MAP = (
        lambda x: -1 * float(x.strip('E')) if 'E' in x else float(x.strip('W'))
    )

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
    def nearest_station(
        cls,
        handler: Any,
        lat: Union[str, float],
        lon: Union[str, float],
        as_df: bool,
    ) -> str:
        """Get nearest station."""
        df = cls.stations(handler=handler, as_df=True)
        closest = cls._nearest(df, lat, lon)
        if as_df:
            return closest
        else:
            return closest.to_records()

    @classmethod
    def metadata(
        cls, handler: Any, station_id: str, as_df: bool
    ) -> Union[pd.DataFrame, dict]:
        """Get station description."""
        req = MetadataRequest.build_request(station_id=station_id)
        resp = handler.handle_request(station_id, req)
        data = MetadataParser.metadata(resp)
        if as_df:
            return pd.DataFrame(data)
        else:
            return data

    @classmethod
    def realtime(
        cls, handler: Any, station_id: str, as_df: bool
    ) -> Union[pd.DataFrame, dict]:
        """Get the available realtime measurements for a station."""
        req = RealtimeRequest.build_request(station_id=station_id)
        resp = handler.handle_request(station_id, req)
        data = RealtimeParser.available_measurements(resp)
        if as_df:
            return pd.DataFrame(data)
        else:
            return data

    @classmethod
    def historical(
        cls, handler: Any, station_id: str, as_df: bool
    ) -> Union[pd.DataFrame, dict]:
        """Get the available historical measurements for a station."""
        req = HistoricalRequest.build_request(station_id=station_id)
        resp = handler.handle_request(station_id, req)
        data = HistoricalParser.available_measurements(resp)
        if as_df:
            return pd.DataFrame(data)
        else:
            return data

    @staticmethod
    def _nearest(df: pd.DataFrame, lat_a: float, lon_a: float):
        def _distance(
            lat_a: float, lon_a: float, lat_b: float, lon_b: float
        ) -> float:
            haversine = (
                0.5
                - cos((lat_b - lat_a) * StaitonsHandler.DEG_TO_RAD) / 2
                + cos(lat_a * StaitonsHandler.DEG_TO_RAD)
                * cos(lat_b * StaitonsHandler.DEG_TO_RAD)
                * (1 - cos((lon_b - lon_a) * StaitonsHandler.DEG_TO_RAD))
                / 2
            )
            return StaitonsHandler.DIAM_OF_EARTH * asin(sqrt(haversine))

        ls = list(df[['Location Lat/Long']].to_records(index=False))
        ls = [
            (
                idx,
                StaitonsHandler.LAT_MAP(r[0].split(' ')[0]),
                StaitonsHandler.LON_MAP(r[0].split(' ')[1]),
            )
            for idx, r in enumerate(ls)
        ]
        closest = min(ls, key=lambda p: _distance(lat_a, lon_a, p[1], p[2]))
        return df.iloc[[closest[0]]]
