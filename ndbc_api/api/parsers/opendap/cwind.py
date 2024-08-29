from typing import List

import pandas as pd

from ndbc_api.api.parsers.opendap._base import BaseParser


class CwindParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = [99.0, 999, 9999, 9999.0, 'MM']

    @classmethod
    def xr_from_responses(cls, responses: List[dict],
                          ) -> pd.DataFrame:
        return super(CwindParser,
                     cls).xr_from_responses(responses)
