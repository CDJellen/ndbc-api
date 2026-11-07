from typing import List, TYPE_CHECKING

from ndbc_api.api.parsers.opendap._base import BaseParser

if TYPE_CHECKING:
    import xarray


class AdcpParser(BaseParser):

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude', 'depth']

    @classmethod
    def nc_from_responses(cls,
                          responses: List[dict],
                          use_timestamp: bool = False) -> 'xarray.Dataset':
        return super(AdcpParser, cls).nc_from_responses(responses, use_timestamp=use_timestamp)
