from datetime import datetime, timedelta
from typing import Any, List, Union

import pandas as pd

from ndbc_api.api.handlers._base import BaseHandler
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


class DataHandler(BaseHandler):
    @classmethod
    def adcp(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """adcp"""
        reqs = AdcpRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = AdcpParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def cwind(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """cwind"""
        reqs = CwindRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = CwindParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def ocean(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """ocean"""
        reqs = OceanRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = OceanParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def spec(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """spec"""
        reqs = SpecRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = SpecParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def stdmet(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """stdmet"""
        reqs = StdmetRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = StdmetParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def supl(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """supl"""
        reqs = SuplRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = SuplParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def swden(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """swden"""
        reqs = SwdenRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = SwdenParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def swdir(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """swdir"""
        reqs = SwdirRequest.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = SwdirParser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def swdir2(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """swdir2"""
        reqs = Swdir2Request.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = Swdir2Parser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def swr1(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """swr1"""
        reqs = Swr1Request.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = Swr1Parser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()

    @classmethod
    def swr2(
        cls,
        handler: Any,
        station_id: Union[int, str],
        cols: List[str] = None,
        start_time: Union[str, datetime] = datetime.now() - timedelta(days=30),
        end_time: Union[str, datetime] = datetime.now(),
        use_timestamp: bool = True,
        as_df: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """swr2"""
        reqs = Swr2Request.build_request(
            station_id=station_id, start_time=start_time, end_time=end_time
        )
        resps = handler.handle_requests(station_id=station_id, reqs=reqs)
        df = Swr2Parser.df_from_responses(
            responses=resps, use_timestamp=use_timestamp
        )
        if cols:
            df = df[[cols]]
        if as_df:
            return df
        else:
            return df.to_records()
