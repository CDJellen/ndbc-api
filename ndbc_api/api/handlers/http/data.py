from datetime import datetime, timedelta
from typing import Any, List

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.parsers.http.adcp import AdcpParser
from ndbc_api.api.parsers.http.cwind import CwindParser
from ndbc_api.api.parsers.http.ocean import OceanParser
from ndbc_api.api.parsers.http.spec import SpecParser
from ndbc_api.api.parsers.http.stdmet import StdmetParser
from ndbc_api.api.parsers.http.supl import SuplParser
from ndbc_api.api.parsers.http.swden import SwdenParser
from ndbc_api.api.parsers.http.swdir import SwdirParser
from ndbc_api.api.parsers.http.swdir2 import Swdir2Parser
from ndbc_api.api.parsers.http.swr1 import Swr1Parser
from ndbc_api.api.parsers.http.swr2 import Swr2Parser
from ndbc_api.api.requests.http.adcp import AdcpRequest
from ndbc_api.api.requests.http.cwind import CwindRequest
from ndbc_api.api.requests.http.ocean import OceanRequest
from ndbc_api.api.requests.http.spec import SpecRequest
from ndbc_api.api.requests.http.stdmet import StdmetRequest
from ndbc_api.api.requests.http.supl import SuplRequest
from ndbc_api.api.requests.http.swden import SwdenRequest
from ndbc_api.api.requests.http.swdir import SwdirRequest
from ndbc_api.api.requests.http.swdir2 import Swdir2Request
from ndbc_api.api.requests.http.swr1 import Swr1Request
from ndbc_api.api.requests.http.swr2 import Swr2Request
from ndbc_api.exceptions import RequestException, ResponseException


class DataHandler(BaseHandler):

    @classmethod
    def adcp(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """adcp"""
        try:
            reqs = AdcpRequest.build_request(station_id=station_id,
                                             start_time=start_time,
                                             end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return AdcpParser.parse_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def cwind(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """cwind"""
        try:
            reqs = CwindRequest.build_request(station_id=station_id,
                                              start_time=start_time,
                                              end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return CwindParser.parse_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def ocean(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """ocean"""
        try:
            reqs = OceanRequest.build_request(station_id=station_id,
                                              start_time=start_time,
                                              end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return OceanParser.parse_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def spec(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """spec"""
        try:
            reqs = SpecRequest.build_request(station_id=station_id,
                                             start_time=start_time,
                                             end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return SpecParser.parse_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def stdmet(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """stdmet"""
        try:
            reqs = StdmetRequest.build_request(station_id=station_id,
                                               start_time=start_time,
                                               end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return StdmetParser.parse_responses(responses=resps,
                                              use_timestamp=use_timestamp)

    @classmethod
    def supl(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """supl"""
        try:
            reqs = SuplRequest.build_request(station_id=station_id,
                                             start_time=start_time,
                                             end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return SuplParser.parse_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def swden(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swden"""
        try:
            reqs = SwdenRequest.build_request(station_id=station_id,
                                              start_time=start_time,
                                              end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return SwdenParser.parse_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def swdir(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swdir"""
        try:
            reqs = SwdirRequest.build_request(station_id=station_id,
                                              start_time=start_time,
                                              end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return SwdirParser.parse_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def swdir2(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swdir2"""
        try:
            reqs = Swdir2Request.build_request(station_id=station_id,
                                               start_time=start_time,
                                               end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return Swdir2Parser.parse_responses(responses=resps,
                                              use_timestamp=use_timestamp)

    @classmethod
    def swr1(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swr1"""
        try:
            reqs = Swr1Request.build_request(station_id=station_id,
                                             start_time=start_time,
                                             end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return Swr1Parser.parse_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def swr2(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> List[dict]:
        """swr2"""
        try:
            reqs = Swr2Request.build_request(station_id=station_id,
                                             start_time=start_time,
                                             end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return Swr2Parser.parse_responses(responses=resps,
                                            use_timestamp=use_timestamp)
