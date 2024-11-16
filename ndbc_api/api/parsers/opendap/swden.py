from typing import List

import pandas as pd

from ndbc_api.api.parsers.opendap._base import BaseParser


class SwdenParser(BaseParser):

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude', 'frequency']

    @classmethod
    def nc_from_responses(cls,
                          responses: List[dict],
                          use_timestamp: bool = False) -> pd.DataFrame:
        return super(SwdenParser, cls).nc_from_responses(responses)
