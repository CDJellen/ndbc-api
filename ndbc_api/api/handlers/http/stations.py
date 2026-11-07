from math import asin, cos, isnan, pi, sqrt
from typing import Any, List, Optional, Union

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.parsers.http.active_stations import ActiveStationsParser
from ndbc_api.api.parsers.http.historical_stations import HistoricalStationsParser
from ndbc_api.api.parsers.http.station_historical import HistoricalParser
from ndbc_api.api.parsers.http.station_metadata import MetadataParser
from ndbc_api.api.parsers.http.station_realtime import RealtimeParser
from ndbc_api.api.requests.http.active_stations import ActiveStationsRequest
from ndbc_api.api.requests.http.historical_stations import HistoricalStationsRequest
from ndbc_api.api.requests.http.station_historical import HistoricalRequest
from ndbc_api.api.requests.http.station_metadata import MetadataRequest
from ndbc_api.api.requests.http.station_realtime import RealtimeRequest
from ndbc_api.exceptions import ParserException, ResponseException


class StationsHandler(BaseHandler):

    DEG_TO_RAD = pi / 180
    DIAM_OF_EARTH = 12756  # km
    LAT_MAP = staticmethod(lambda val: StationsHandler._parse_lat(val))
    LON_MAP = staticmethod(lambda val: StationsHandler._parse_lon(val))
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
        handler: Any = None,
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
        stations: Optional[Any] = None,
    ) -> Union[str, List[str]]:
        """Get nearest station(s) from specified lat/lon with optional capability filters."""
        if lat is None or lon is None:
            raise ValueError('lat and lon must be specified.')
        if k < 1:
            raise ValueError('k must be >= 1.')
        lat_f = cls.LAT_MAP(lat)
        lon_f = cls.LON_MAP(lon)
        df = stations if stations is not None else cls.stations(handler=handler)
        try:
            filtered = cls._filter_stations(
                df,
                met=met,
                currents=currents,
                waterquality=waterquality,
                dart=dart,
                owner=owner,
                program=program,
                station_type=station_type,
            )
            if k == 1:
                closest = cls._nearest(filtered, lat_f, lon_f)
                return closest.get('Station', 'UNK')
            top_k = cls._nearest_k(filtered, lat_f, lon_f, k=k)
            return [s.get('Station', 'UNK') for s in top_k]
        except (TypeError, KeyError, ValueError) as e:
            raise ParserException from e

    @classmethod
    def radial_search(
        cls,
        handler: Any = None,
        lat: Union[str, float, None] = None,
        lon: Union[str, float, None] = None,
        radius: float = -1,
        units: str = 'km',
        met: Optional[bool] = None,
        currents: Optional[bool] = None,
        waterquality: Optional[bool] = None,
        dart: Optional[bool] = None,
        owner: Optional[str] = None,
        program: Optional[str] = None,
        station_type: Optional[str] = None,
        limit: Optional[int] = None,
        stations: Optional[Any] = None,
    ) -> List[dict]:
        """Get stations within <radius> of the specified lat/lon with optional filters."""
        if lat is None or lon is None:
            raise ValueError('lat and lon must be specified.')
        if units not in cls.UNITS:
            raise ValueError(
                f'Invalid unit: {units}, must be one of {cls.UNITS}.')
        if radius < 0:
            raise ValueError(f'Invalid radius: {radius}, must be non-negative.')
        if limit is not None and limit < 1:
            raise ValueError('limit must be >= 1.')
        lat_f = cls.LAT_MAP(lat)
        lon_f = cls.LON_MAP(lon)
        # pass the radius in km
        if units == 'nm':
            radius = radius * 1.852
        elif units == 'mi':
            radius = radius * 1.60934

        df = stations if stations is not None else cls.stations(handler=handler)
        try:
            filtered = cls._filter_stations(
                df,
                met=met,
                currents=currents,
                waterquality=waterquality,
                dart=dart,
                owner=owner,
                program=program,
                station_type=station_type,
            )
            stations_in_radius = cls._radial_search(filtered, lat_f, lon_f, radius)
            if limit is not None:
                stations_in_radius = stations_in_radius[:limit]
        except (TypeError, KeyError, ValueError) as e:
            raise ParserException from e
        return stations_in_radius

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

    @staticmethod
    def _parse_lat(val: Union[str, float, int]) -> float:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            res = float(val)
        elif isinstance(val, str):
            s = val.strip().upper().replace('°', '').strip()
            sign = -1.0 if s.endswith('S') else 1.0
            s = s.rstrip('NS').strip()
            res = sign * float(s)
        else:
            raise TypeError(f'Invalid latitude type: {type(val).__name__}')
        if isnan(res) or res < -90.0 or res > 90.0:
            raise ValueError(f'Latitude {val} out of bounds [-90, 90].')
        return res

    @staticmethod
    def _parse_lon(val: Union[str, float, int]) -> float:
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            res = float(val)
        elif isinstance(val, str):
            s = val.strip().upper().replace('°', '').strip()
            sign = -1.0 if s.endswith('W') else 1.0
            s = s.rstrip('EW').strip()
            res = sign * float(s)
        else:
            raise TypeError(f'Invalid longitude type: {type(val).__name__}')
        if isnan(res) or res < -180.0 or res > 360.0:
            raise ValueError(f'Longitude {val} out of bounds [-180, 360].')
        return res

    @staticmethod
    def _distance(lat_a: float, lon_a: float, lat_b: float,
                  lon_b: float) -> float:
        haversine = (0.5 - cos(
            (lat_b - lat_a) * StationsHandler.DEG_TO_RAD) / 2 +
                     cos(lat_a * StationsHandler.DEG_TO_RAD) *
                     cos(lat_b * StationsHandler.DEG_TO_RAD) * (1 - cos(
                         (lon_b - lon_a) * StationsHandler.DEG_TO_RAD)) / 2)
        haversine = min(1.0, max(0.0, haversine))
        return StationsHandler.DIAM_OF_EARTH * asin(sqrt(haversine))

    @staticmethod
    def _filter_stations(
        stations: Any,
        met: Optional[bool] = None,
        currents: Optional[bool] = None,
        waterquality: Optional[bool] = None,
        dart: Optional[bool] = None,
        owner: Optional[str] = None,
        program: Optional[str] = None,
        station_type: Optional[str] = None,
    ) -> List[dict]:
        """Filter station catalog records by sensor capabilities and metadata."""
        if hasattr(stations, 'to_dict'):
            stations = stations.to_dict(orient='records')
        elif hasattr(stations, 'to_dicts'):
            stations = stations.to_dicts()
        if all(
            v is None
            for v in (met, currents, waterquality, dart, owner, program, station_type)
        ):
            return list(stations)

        filtered = []
        for s in stations:
            if met is not None and bool(s.get('Includes Meteorology')) is not bool(met):
                continue
            if currents is not None and bool(s.get('Includes Currents')) is not bool(currents):
                continue
            if waterquality is not None and bool(s.get('Includes Water Quality')) is not bool(waterquality):
                continue
            if dart is not None and bool(s.get('DART Program')) is not bool(dart):
                continue
            if owner is not None and owner.lower() not in str(s.get('Owner') or '').lower():
                continue
            if program is not None and program.lower() not in str(s.get('Program') or '').lower():
                continue
            if station_type is not None and station_type.lower() not in str(s.get('Type') or '').lower():
                continue
            filtered.append(s)
        return filtered

    @staticmethod
    def _nearest_k(
        stations: Any,
        lat_a: float,
        lon_a: float,
        k: int = 1,
    ) -> List[dict]:
        """Get the top `k` nearest stations from specified `float`-valued lat/lon."""
        if hasattr(stations, 'to_dict'):
            stations = stations.to_dict(orient='records')
        elif hasattr(stations, 'to_dicts'):
            stations = stations.to_dicts()
        scored = []
        for s in stations:
            lat_val = s.get('Lat')
            lon_val = s.get('Lon')
            if lat_val is None or lon_val is None or (isinstance(lat_val, float) and lat_val != lat_val):
                continue
            try:
                dist = StationsHandler._distance(
                    lat_a,
                    lon_a,
                    StationsHandler._parse_lat(lat_val),
                    StationsHandler._parse_lon(lon_val),
                )
                scored.append((dist, str(s.get('Station') or ''), s))
            except (ValueError, TypeError):
                continue
        scored.sort(key=lambda item: (item[0], item[1]))
        return [item[2] for item in scored[:k]]

    @staticmethod
    def _nearest(stations: Any, lat_a: float, lon_a: float) -> dict:
        """Get the nearest station from specified `float`-valued lat/lon."""
        top = StationsHandler._nearest_k(stations, lat_a, lon_a, k=1)
        return top[0] if top else {}

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
                dist = StationsHandler._distance(
                    lat_a,
                    lon_a,
                    StationsHandler._parse_lat(lat_val),
                    StationsHandler._parse_lon(lon_val),
                )
                if dist <= radius:
                    s_copy = s.copy()
                    s_copy['distance'] = dist
                    results.append(s_copy)
            except (ValueError, TypeError):
                continue
        results.sort(key=lambda x: x.get('distance', 0.0))
        return results
