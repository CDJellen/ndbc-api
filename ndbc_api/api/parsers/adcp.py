from typing import List

import pandas as pd

from ndbc_api.api.parsers._base import BaseParser


class AdcpParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = None

    @classmethod
    def df_from_responses(cls, responses: List[dict]) -> pd.DataFrame:
        return super(AdcpParser, cls).df_from_responses(responses)
