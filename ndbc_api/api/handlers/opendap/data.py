from datetime import datetime, timedelta
from typing import Any

import netCDF4 as nc

from ndbc_api.api.handlers._base import BaseHandler
from ndbc_api.api.parsers.opendap.adcp import AdcpParser
from ndbc_api.api.parsers.opendap.cwind import CwindParser
from ndbc_api.api.parsers.opendap.ocean import OceanParser
from ndbc_api.api.parsers.opendap.pwind import PwindParser
from ndbc_api.api.parsers.opendap.stdmet import StdmetParser
from ndbc_api.api.parsers.opendap.swden import SwdenParser
from ndbc_api.api.parsers.opendap.wlevel import WlevelParser
from ndbc_api.api.requests.opendap.adcp import AdcpRequest
from ndbc_api.api.requests.opendap.cwind import CwindRequest
from ndbc_api.api.requests.opendap.ocean import OceanRequest
from ndbc_api.api.requests.opendap.pwind import PwindRequest
from ndbc_api.api.requests.opendap.stdmet import StdmetRequest
from ndbc_api.api.requests.opendap.swden import SwdenRequest
from ndbc_api.api.requests.opendap.wlevel import WlevelRequest
from ndbc_api.exceptions import RequestException, ResponseException


class OpenDapDataHandler(BaseHandler):

    @classmethod
    def adcp(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
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
        return AdcpParser.nc_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def cwind(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
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
        return CwindParser.nc_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def ocean(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
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
        return OceanParser.nc_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)

    @classmethod
    def pwind(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
        """pwind"""
        try:
            reqs = PwindRequest.build_request(station_id=station_id,
                                             start_time=start_time,
                                             end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return PwindParser.nc_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def stdmet(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
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
        return StdmetParser.nc_from_responses(responses=resps,
                                              use_timestamp=use_timestamp)

    @classmethod
    def swden(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
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
        return SwdenParser.nc_from_responses(responses=resps,
                                            use_timestamp=use_timestamp)

    @classmethod
    def wlevel(
        cls,
        handler: Any,
        station_id: str,
        start_time: datetime = datetime.now() - timedelta(days=30),
        end_time: datetime = datetime.now(),
        use_timestamp: bool = True,
    ) -> 'nc.Dataset':
        """wlevel"""
        try:
            reqs = WlevelRequest.build_request(station_id=station_id,
                                              start_time=start_time,
                                              end_time=end_time)
        except Exception as e:
            raise RequestException('Failed to build request.') from e
        try:
            resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        except Exception as e:
            raise ResponseException('Failed to execute requests.') from e
        return WlevelParser.nc_from_responses(responses=resps,
                                             use_timestamp=use_timestamp)
