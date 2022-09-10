from typing import List

import pandas as pd

from ndbc_api.api.parsers._base import BaseParser


class SpecParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = ['N/A']

    @classmethod
    def df_from_responses(cls, responses: List[dict],
                          use_timestamp: bool) -> pd.DataFrame:
        return super(SpecParser, cls).df_from_responses(responses,
                                                        use_timestamp)
