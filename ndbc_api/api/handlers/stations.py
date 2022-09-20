from math import asin, cos, pi, sqrt
from typing import Any, Union

import pandas as pd

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.parsers.station_historical import HistoricalParser
from ndbc_api.api.parsers.station_metadata import MetadataParser
from ndbc_api.api.parsers.station_realtime import RealtimeParser
from ndbc_api.api.parsers.stations import StationsParser
from ndbc_api.api.requests.station_historical import HistoricalRequest
from ndbc_api.api.requests.station_metadata import MetadataRequest
from ndbc_api.api.requests.station_realtime import RealtimeRequest
from ndbc_api.api.requests.stations import StationsRequest
from ndbc_api.exceptions import ParserException, ResponseException


class StationsHandler(BaseHandler):

    DEG_TO_RAD = pi / 180
    DIAM_OF_EARTH = 12756
    LAT_MAP = (lambda x: -1 * float(x.strip('S'))
               if 'S' in x else float(x.strip('N')))
    LON_MAP = (lambda x: -1 * float(x.strip('E'))
               if 'E' in x else float(x.strip('W')))

    @classmethod
    def stations(cls, handler: Any) -> pd.DataFrame:
        """Get all stations from NDBC."""
        req = StationsRequest.build_request()
        try:
            resp = handler.handle_request('stn', req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return StationsParser.df_from_responses([resp])

    @classmethod
    def nearest_station(
        cls,
        handler: Any,
        lat: Union[str, float],
        lon: Union[str, float],
    ) -> str:
        """Get nearest station from specified lat/lon."""
        df = cls.stations(handler=handler)
        if isinstance(lat, str):
            lat = StationsHandler.LAT_MAP(lat)
        if isinstance(lon, str):
            lon = StationsHandler.LON_MAP(lon)
        try:
            closest = cls._nearest(df, lat, lon)
        except (TypeError, KeyError, ValueError) as e:
            raise ParserException from e
        closest = closest.to_dict().get('Station', {'UNK': 'UNK'})
        return list(closest.values())[0]

    @classmethod
    def metadata(cls, handler: Any, station_id: str) -> pd.DataFrame:
        """Get station description."""
        req = MetadataRequest.build_request(station_id=station_id)
        try:
            resp = handler.handle_request(station_id, req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return MetadataParser.metadata(resp)

    @classmethod
    def realtime(cls, handler: Any, station_id: str) -> pd.DataFrame:
        """Get the available realtime measurements for a station."""
        req = RealtimeRequest.build_request(station_id=station_id)
        try:
            resp = handler.handle_request(station_id, req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return RealtimeParser.available_measurements(resp)

    @classmethod
    def historical(cls, handler: Any,
                   station_id: str) -> Union[pd.DataFrame, dict]:
        """Get the available historical measurements for a station."""
        req = HistoricalRequest.build_request(station_id=station_id)
        try:
            resp = handler.handle_request(station_id, req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return HistoricalParser.available_measurements(resp)

    """ PRIVATE """

    @staticmethod
    def _nearest(df: pd.DataFrame, lat_a: float, lon_a: float):
        """Get the nearest station from specified `float`-valued lat/lon."""

        def _distance(lat_a: float, lon_a: float, lat_b: float,
                      lon_b: float) -> float:
            haversine = (0.5 - cos(
                (lat_b - lat_a) * StationsHandler.DEG_TO_RAD) / 2 +
                         cos(lat_a * StationsHandler.DEG_TO_RAD) *
                         cos(lat_b * StationsHandler.DEG_TO_RAD) * (1 - cos(
                             (lon_b - lon_a) * StationsHandler.DEG_TO_RAD)) / 2)
            return StationsHandler.DIAM_OF_EARTH * asin(sqrt(haversine))

        ls = list(df[['Location Lat/Long']].to_records(index=False))
        ls = [(
            idx,
            StationsHandler.LAT_MAP(r[0].split(' ')[0]),
            StationsHandler.LON_MAP(r[0].split(' ')[1]),
        ) for idx, r in enumerate(ls)]
        closest = min(ls, key=lambda p: _distance(lat_a, lon_a, p[1], p[2]))
        return df.iloc[[closest[0]]]
