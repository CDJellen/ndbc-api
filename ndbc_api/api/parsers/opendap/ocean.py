from typing import List

import pandas as pd

from ndbc_api.api.parsers.opendap._base import BaseParser


class OceanParser(BaseParser):

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude', 'instrument', 'water_depth']

    @classmethod
    def nc_from_responses(cls,
                          responses: List[dict],
                          use_timestamp: bool = False) -> pd.DataFrame:
        return super(OceanParser, cls).nc_from_responses(responses, use_timestamp=use_timestamp)
