from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.parsers.adcp import AdcpParser
from ndbc_api.api.parsers.cwind import CwindParser
from ndbc_api.api.parsers.ocean import OceanParser
from ndbc_api.api.parsers.spec import SpecParser
from ndbc_api.api.parsers.stdmet import StdmetParser
from ndbc_api.api.parsers.supl import SuplParser
from ndbc_api.api.parsers.swden import SwdenParser
from ndbc_api.api.parsers.swdir import SwdirParser
from ndbc_api.api.parsers.swdir2 import Swdir2Parser
from ndbc_api.api.parsers.swr1 import Swr1Parser
from ndbc_api.api.parsers.swr2 import Swr2Parser
from ndbc_api.api.requests.adcp import AdcpRequest
from ndbc_api.api.requests.cwind import CwindRequest
from ndbc_api.api.requests.ocean import OceanRequest
from ndbc_api.api.requests.spec import SpecRequest
from ndbc_api.api.requests.stdmet import StdmetRequest
from ndbc_api.api.requests.supl import SuplRequest
from ndbc_api.api.requests.swden import SwdenRequest
from ndbc_api.api.requests.swdir import SwdirRequest
from ndbc_api.api.requests.swdir2 import Swdir2Request
from ndbc_api.api.requests.swr1 import Swr1Request
from ndbc_api.api.requests.swr2 import Swr2Request
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
    ) -> pd.DataFrame:
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
        return AdcpParser.df_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def cwind(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return CwindParser.df_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def ocean(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return OceanParser.df_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def spec(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return SpecParser.df_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def stdmet(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return StdmetParser.df_from_responses(responses=resps,
                                              use_timestamp=use_timestamp)

    @classmethod
    def supl(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return SuplParser.df_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def swden(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return SwdenParser.df_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def swdir(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return SwdirParser.df_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def swdir2(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return Swdir2Parser.df_from_responses(responses=resps,
                                              use_timestamp=use_timestamp)

    @classmethod
    def swr1(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return Swr1Parser.df_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def swr2(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> pd.DataFrame:
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
        return Swr2Parser.df_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)
