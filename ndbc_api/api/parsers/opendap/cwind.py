from typing import List

import pandas as pd

from ndbc_api.api.parsers.opendap._base import BaseParser


class CwindParser(BaseParser):

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude', 'instrument', 'water_depth']

    @classmethod
    def nc_from_responses(cls, responses: List[dict], use_timestamp: bool = False) -> pd.DataFrame:
        return super(CwindParser,
                     cls).nc_from_responses(responses)
