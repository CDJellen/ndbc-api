from datetime import datetime, timedelta
from typing import Any, List, Optional, Tuple

from ndbc_api.api.dispatch import HTTP_DISPATCH
from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.exceptions import RequestException, ResponseException


class DataHandler(BaseHandler):

    @classmethod
    def adcp(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
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
    ) -> List[dict]:
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
    ) -> List[dict]:
        """ocean"""
        return cls._execute('ocean', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def spec(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """spec"""
        return cls._execute('spec', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def stdmet(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """stdmet"""
        return cls._execute('stdmet', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def supl(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """supl"""
        return cls._execute('supl', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def swden(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swden"""
        return cls._execute('swden', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def swdir(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swdir"""
        return cls._execute('swdir', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def swdir2(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swdir2"""
        return cls._execute('swdir2', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def swr1(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swr1"""
        return cls._execute('swr1', handler, station_id, start_time, end_time, use_timestamp)

    @classmethod
    def swr2(
        cls,
        handler: Any,
        station_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swr2"""
        return cls._execute('swr2', handler, station_id, start_time, end_time, use_timestamp)

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
    ) -> List[dict]:
        start_time, end_time = cls._resolve_time_range(start_time, end_time)
        request_builder, parser = HTTP_DISPATCH[mode]
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
        return parser.parse_responses(responses=resps, use_timestamp=use_timestamp)
