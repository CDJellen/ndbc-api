from typing import List

import netCDF4 as nc


from ndbc_api.api.parsers.opendap._base import BaseParser


class PwindParser(BaseParser):

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude', 'instrument', 'water_depth']

    @classmethod
    def nc_from_responses(cls, responses: List[dict], use_timestamp: bool = False) -> 'nc.Dataset':
        return super(PwindParser,
                     cls).nc_from_responses(responses)
