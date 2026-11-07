"""Unified mode dispatch registry for HTTP and OpenDAP pipelines.

Provides the single source of truth mapping each supported NDBC measurement
mode to its corresponding `(RequestBuilder, Parser)` pair.
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type, Union

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
from ndbc_api.exceptions import RequestException, TimestampException
from ndbc_api.utilities.data_helpers import handle_timestamp, parse_station_id

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


def build_requests(
    station_id: Union[int, str, None] = None,
    mode: Union[str, None] = None,
    start_time: Union[str, datetime, None] = None,
    end_time: Union[str, datetime, None] = None,
    use_opendap: bool = False,
    as_xarray_dataset: Optional[bool] = None,
    station_ids: Union[Sequence[Union[int, str]], None] = None,
    modes: Union[List[str], None] = None,
) -> Union[List[str], Dict[str, Any]]:
    """Build the ordered list of NDBC/THREDDS archive URLs for a query without network I/O."""
    if station_id is None and station_ids is None:
        raise ValueError('Both `station_id` and `station_ids` are `None`.')
    if station_id is not None and station_ids is not None:
        raise ValueError('`station_id` and `station_ids` cannot both be specified.')
    if mode is None and modes is None:
        raise ValueError('Both `mode` and `modes` are `None`.')
    if mode is not None and modes is not None:
        raise ValueError('`mode` and `modes` cannot both be specified.')
    if modes is not None:
        if not isinstance(modes, list):
            raise ValueError('`modes` must be a list of strings.')
        if any(not isinstance(m, str) for m in modes):
            raise ValueError('All elements in `modes` must be strings.')
        if any(m == 'hfradar' for m in modes):
            raise ValueError(
                'HF radar data cannot be requested with `modes`. '
                'Please use `mode` to specify a single `hfradar` mode.'
            )

    if as_xarray_dataset is not None:
        use_opendap = as_xarray_dataset
    now = datetime.now()
    start_dt = handle_timestamp(
        start_time if start_time is not None else now - timedelta(days=30)
    )
    end_dt = handle_timestamp(
        end_time if end_time is not None else now
    )
    if start_dt > end_dt:
        raise TimestampException(
            f'`start_time` ({start_dt}) must be <= `end_time` ({end_dt}).'
        )

    if station_id is not None and mode is not None:
        sid = parse_station_id(station_id)
        return build_mode_requests(
            mode=mode,
            station_id=sid,
            start_time=start_dt,
            end_time=end_dt,
            use_opendap=use_opendap,
        )

    sids = [
        parse_station_id(s)
        for s in (station_ids if station_ids is not None else [station_id])
    ]
    ms = list(modes if modes is not None else [mode])

    if station_ids is not None and mode is not None:
        return {
            sid: build_mode_requests(
                mode=mode,
                station_id=sid,
                start_time=start_dt,
                end_time=end_dt,
                use_opendap=use_opendap,
            )
            for sid in sids
        }
    if station_id is not None and modes is not None:
        return {
            m: build_mode_requests(
                mode=m,
                station_id=sids[0],
                start_time=start_dt,
                end_time=end_dt,
                use_opendap=use_opendap,
            )
            for m in ms
        }
    return {
        sid: {
            m: build_mode_requests(
                mode=m,
                station_id=sid,
                start_time=start_dt,
                end_time=end_dt,
                use_opendap=use_opendap,
            )
            for m in ms
        }
        for sid in sids
    }


