from math import asin, cos, pi, sqrt
from typing import Any, Union, List

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.parsers.http.station_historical import HistoricalParser
from ndbc_api.api.parsers.http.station_metadata import MetadataParser
from ndbc_api.api.parsers.http.station_realtime import RealtimeParser
from ndbc_api.api.parsers.http.active_stations import ActiveStationsParser
from ndbc_api.api.parsers.http.historical_stations import HistoricalStationsParser
from ndbc_api.api.requests.http.station_historical import HistoricalRequest
from ndbc_api.api.requests.http.station_metadata import MetadataRequest
from ndbc_api.api.requests.http.station_realtime import RealtimeRequest
from ndbc_api.api.requests.http.active_stations import ActiveStationsRequest
from ndbc_api.api.requests.http.historical_stations import HistoricalStationsRequest
from ndbc_api.exceptions import ParserException, ResponseException


class StationsHandler(BaseHandler):

    DEG_TO_RAD = pi / 180
    DIAM_OF_EARTH = 12756  # km
    LAT_MAP = (lambda x: -1 * float(x.strip('S'))
               if 'S' in x else float(x.strip('N')))
    LON_MAP = (lambda x: -1 * float(x.strip('W'))
               if 'W' in x else float(x.strip('E')))
    UNITS = ('nm', 'km', 'mi')

    @classmethod
    def stations(cls, handler: Any) -> List[dict]:
        """Get all active stations from NDBC."""
        req = ActiveStationsRequest.build_request()
        try:
            resp = handler.handle_request('stn_active', req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return ActiveStationsParser.parse_response(resp, use_timestamp=False)

    @classmethod
    def historical_stations(cls, handler: Any) -> List[dict]:
        """Get historical stations from NDBC."""
        req = HistoricalStationsRequest.build_request()
        try:
            resp = handler.handle_request('stn_historical', req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return HistoricalStationsParser.parse_response(resp, use_timestamp=False)

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
        return closest.get('Station', 'UNK')

    @classmethod
    def radial_search(
        cls,
        handler: Any,
        lat: Union[str, float],
        lon: Union[str, float],
        radius: float,
        units: str = 'km',
    ) -> List[dict]:
        """Get stations within <radius> of the specified lat/lon."""
        if units not in cls.UNITS:
            raise ValueError(
                f'Invalid unit: {units}, must be one of {cls.UNITS}.')
        if radius < 0:
            raise ValueError(f'Invalid radius: {radius}, must be non-negative.')
        # pass the radius in km
        if units == 'nm':
            radius = radius * 1.852
        elif units == 'mi':
            radius = radius * 1.60934

        df = cls.stations(handler=handler)
        if isinstance(lat, str):
            lat = StationsHandler.LAT_MAP(lat)
        if isinstance(lon, str):
            lon = StationsHandler.LON_MAP(lon)
        try:
            sations_in_radius = cls._radial_search(df, lat, lon, radius)
        except (TypeError, KeyError, ValueError) as e:
            raise ParserException from e
        return sations_in_radius

    @classmethod
    def metadata(cls, handler: Any, station_id: str) -> dict:
        """Get station description."""
        req = MetadataRequest.build_request(station_id=station_id)
        try:
            resp = handler.handle_request(station_id, req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return MetadataParser.metadata(resp)

    @classmethod
    def realtime(cls, handler: Any, station_id: str) -> dict:
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
                   station_id: str) -> dict:
        """Get the available historical measurements for a station."""
        req = HistoricalRequest.build_request(station_id=station_id)
        try:
            resp = handler.handle_request(station_id, req)
        except (AttributeError, ValueError, TypeError) as e:
            raise ResponseException(
                'Failed to execute `station` request.') from e
        return HistoricalParser.available_measurements(resp)

    """ PRIVATE """

    def _distance(lat_a: float, lon_a: float, lat_b: float,
                  lon_b: float) -> float:
        haversine = (0.5 - cos(
            (lat_b - lat_a) * StationsHandler.DEG_TO_RAD) / 2 +
                     cos(lat_a * StationsHandler.DEG_TO_RAD) *
                     cos(lat_b * StationsHandler.DEG_TO_RAD) * (1 - cos(
                         (lon_b - lon_a) * StationsHandler.DEG_TO_RAD)) / 2)
        return StationsHandler.DIAM_OF_EARTH * asin(sqrt(haversine))

    @staticmethod
    def _nearest(stations: Any, lat_a: float, lon_a: float) -> dict:
        """Get the nearest station from specified `float`-valued lat/lon."""
        if hasattr(stations, 'to_dict'):
            stations = stations.to_dict(orient='records')
        elif hasattr(stations, 'to_dicts'):
            stations = stations.to_dicts()
        closest = None
        min_dist = float('inf')
        for s in stations:
            lat_val = s.get('Lat')
            lon_val = s.get('Lon')
            if lat_val is None or lon_val is None or (isinstance(lat_val, float) and lat_val != lat_val):
                continue
            try:
                dist = StationsHandler._distance(lat_a, lon_a, float(lat_val), float(lon_val))
                if dist < min_dist:
                    min_dist = dist
                    closest = s
            except (ValueError, TypeError):
                continue
        return closest or {}

    @staticmethod
    def _radial_search(stations: Any, lat_a: float, lon_a: float,
                       radius: float) -> List[dict]:
        """Get the stations within radius km from specified `float`-valued lat/lon."""
        if hasattr(stations, 'to_dict'):
            stations = stations.to_dict(orient='records')
        elif hasattr(stations, 'to_dicts'):
            stations = stations.to_dicts()
        results = []
        for s in stations:
            lat_val = s.get('Lat')
            lon_val = s.get('Lon')
            if lat_val is None or lon_val is None or (isinstance(lat_val, float) and lat_val != lat_val):
                continue
            try:
                dist = StationsHandler._distance(lat_a, lon_a, float(lat_val), float(lon_val))
                if dist <= radius:
                    s_copy = s.copy()
                    s_copy['distance'] = dist
                    results.append(s_copy)
            except (ValueError, TypeError):
                continue
        results.sort(key=lambda x: x.get('distance', 0.0))
        return results
