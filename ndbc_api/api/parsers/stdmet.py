from typing import List

import pandas as pd

from ndbc_api.api.parsers._base import BaseParser


class StdmetParser(BaseParser):

    INDEX_COL = 0

    @classmethod
    def df_from_responses(cls, responses: List[dict]) -> pd.DataFrame:
        return super(StdmetParser, cls).df_from_responses(responses)
