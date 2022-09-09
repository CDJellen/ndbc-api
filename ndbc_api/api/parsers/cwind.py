from typing import List

import pandas as pd

from ndbc_api.api.parsers._base import BaseParser


class CwindParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = [99.0, 999, 9999, 9999.0, 'MM']

    @classmethod
    def df_from_responses(cls, responses: List[dict],
                          use_timestamp: bool) -> pd.DataFrame:
        return super(CwindParser,
                     cls).df_from_responses(responses, use_timestamp)
