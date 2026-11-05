"""Unified mode dispatch registry for HTTP and OpenDAP pipelines.

Provides the single source of truth mapping each supported NDBC measurement
mode to its corresponding `(RequestBuilder, Parser)` pair.
"""
from datetime import datetime
from typing import Dict, List, Tuple, Type

from ndbc_api.api.parsers.http.adcp import AdcpParser as HttpAdcpParser
from ndbc_api.api.parsers.http.cwind import CwindParser as HttpCwindParser
from ndbc_api.api.parsers.http.ocean import OceanParser as HttpOceanParser
from ndbc_api.api.parsers.http.spec import SpecParser as HttpSpecParser
from ndbc_api.api.parsers.http.stdmet import StdmetParser as HttpStdmetParser
from ndbc_api.api.parsers.http.supl import SuplParser as HttpSuplParser
from ndbc_api.api.parsers.http.swden import SwdenParser as HttpSwdenParser
from ndbc_api.api.parsers.http.swdir import SwdirParser as HttpSwdirParser
from ndbc_api.api.parsers.http.swdir2 import Swdir2Parser as HttpSwdir2Parser
from ndbc_api.api.parsers.http.swr1 import Swr1Parser as HttpSwr1Parser
from ndbc_api.api.parsers.http.swr2 import Swr2Parser as HttpSwr2Parser
from ndbc_api.api.parsers.opendap.adcp import AdcpParser as OpendapAdcpParser
from ndbc_api.api.parsers.opendap.cwind import CwindParser as OpendapCwindParser
from ndbc_api.api.parsers.opendap.hfradar import HfradarParser as OpendapHfradarParser
from ndbc_api.api.parsers.opendap.ocean import OceanParser as OpendapOceanParser
from ndbc_api.api.parsers.opendap.pwind import PwindParser as OpendapPwindParser
from ndbc_api.api.parsers.opendap.stdmet import StdmetParser as OpendapStdmetParser
from ndbc_api.api.parsers.opendap.swden import SwdenParser as OpendapSwdenParser
from ndbc_api.api.parsers.opendap.wlevel import WlevelParser as OpendapWlevelParser
from ndbc_api.api.requests.http.adcp import AdcpRequest as HttpAdcpRequest
from ndbc_api.api.requests.http.cwind import CwindRequest as HttpCwindRequest
from ndbc_api.api.requests.http.ocean import OceanRequest as HttpOceanRequest
from ndbc_api.api.requests.http.spec import SpecRequest as HttpSpecRequest
from ndbc_api.api.requests.http.stdmet import StdmetRequest as HttpStdmetRequest
from ndbc_api.api.requests.http.supl import SuplRequest as HttpSuplRequest
from ndbc_api.api.requests.http.swden import SwdenRequest as HttpSwdenRequest
from ndbc_api.api.requests.http.swdir import SwdirRequest as HttpSwdirRequest
from ndbc_api.api.requests.http.swdir2 import Swdir2Request as HttpSwdir2Request
from ndbc_api.api.requests.http.swr1 import Swr1Request as HttpSwr1Request
from ndbc_api.api.requests.http.swr2 import Swr2Request as HttpSwr2Request
from ndbc_api.api.requests.opendap.adcp import AdcpRequest as OpendapAdcpRequest
from ndbc_api.api.requests.opendap.cwind import CwindRequest as OpendapCwindRequest
from ndbc_api.api.requests.opendap.hfradar import HfradarRequest as OpendapHfradarRequest
from ndbc_api.api.requests.opendap.ocean import OceanRequest as OpendapOceanRequest
from ndbc_api.api.requests.opendap.pwind import PwindRequest as OpendapPwindRequest
from ndbc_api.api.requests.opendap.stdmet import StdmetRequest as OpendapStdmetRequest
from ndbc_api.api.requests.opendap.swden import SwdenRequest as OpendapSwdenRequest
from ndbc_api.api.requests.opendap.wlevel import WlevelRequest as OpendapWlevelRequest
from ndbc_api.exceptions import RequestException

HTTP_DISPATCH: Dict[str, Tuple[Type, Type]] = {
    'adcp': (HttpAdcpRequest, HttpAdcpParser),
    'cwind': (HttpCwindRequest, HttpCwindParser),
    'ocean': (HttpOceanRequest, HttpOceanParser),
    'spec': (HttpSpecRequest, HttpSpecParser),
    'stdmet': (HttpStdmetRequest, HttpStdmetParser),
    'supl': (HttpSuplRequest, HttpSuplParser),
    'swden': (HttpSwdenRequest, HttpSwdenParser),
    'swdir': (HttpSwdirRequest, HttpSwdirParser),
    'swdir2': (HttpSwdir2Request, HttpSwdir2Parser),
    'swr1': (HttpSwr1Request, HttpSwr1Parser),
    'swr2': (HttpSwr2Request, HttpSwr2Parser),
}

OPENDAP_DISPATCH: Dict[str, Tuple[Type, Type]] = {
    'adcp': (OpendapAdcpRequest, OpendapAdcpParser),
    'cwind': (OpendapCwindRequest, OpendapCwindParser),
    'ocean': (OpendapOceanRequest, OpendapOceanParser),
    'pwind': (OpendapPwindRequest, OpendapPwindParser),
    'stdmet': (OpendapStdmetRequest, OpendapStdmetParser),
    'swden': (OpendapSwdenRequest, OpendapSwdenParser),
    'wlevel': (OpendapWlevelRequest, OpendapWlevelParser),
    'hfradar': (OpendapHfradarRequest, OpendapHfradarParser),
}


def build_mode_requests(
    mode: str,
    station_id: str,
    start_time: datetime,
    end_time: datetime,
    use_opendap: bool = False,
) -> List[str]:
    """Build the ordered list of NDBC/THREDDS request URLs for a mode and time window."""
    registry = OPENDAP_DISPATCH if use_opendap else HTTP_DISPATCH
    entry = registry.get(mode)
    if not entry:
        raise RequestException(f'Mode {mode} is not available.')
    request_builder, _ = entry
    try:
        return request_builder.build_request(
            station_id=station_id,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as e:
        raise RequestException('Failed to build request.') from e

