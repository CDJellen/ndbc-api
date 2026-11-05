from datetime import datetime, timedelta
from typing import Any, Optional, Tuple

try:
    import xarray
except ImportError:
    xarray = None

from ndbc_api.api.dispatch import OPENDAP_DISPATCH
from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.exceptions import ParserException, RequestException, ResponseException


class OpenDapDataHandler(BaseHandler):

    @classmethod
    def adcp(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """adcp"""
        return cls._execute('adcp', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def cwind(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """cwind"""
        return cls._execute('cwind', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def ocean(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """ocean"""
        return cls._execute('ocean', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def pwind(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """pwind"""
        return cls._execute('pwind', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def stdmet(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """stdmet"""
        return cls._execute('stdmet', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def swden(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """swden"""
        return cls._execute('swden', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def wlevel(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """wlevel"""
        return cls._execute('wlevel', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def hfradar(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        """hfradar"""
        return cls._execute('hfradar', handler, station_id, start_time, end_time, use_timestamp)

    """ PRIVATE """

    @staticmethod
    def _resolve_time_range(
        start_time: Optional[datetime],
        end_time: Optional[datetime],
    ) -> Tuple[datetime, datetime]:
        now = datetime.now()
        if start_time is None:
            start_time = now - timedelta(days=30)
        if end_time is None:
            end_time = now
        return start_time, end_time

    @classmethod
    def _execute(
        cls,
        mode: str,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> 'xarray.Dataset':
        start_time, end_time = cls._resolve_time_range(start_time, end_time)
        request_builder, parser = OPENDAP_DISPATCH[mode]
        try:
            reqs = request_builder.build_request(
                station_id=station_id,
                start_time=start_time,
                end_time=end_time,
            )
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        try:
            return parser.nc_from_responses(responses=resps, use_timestamp=use_timestamp)
        except ParserException as e:
            raise ResponseException('Failed to parse response.') from e
