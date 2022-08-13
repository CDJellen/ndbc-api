from typing import List

import pandas as pd

from ndbc_api.api.parsers._base import BaseParser


class SwdenParser(BaseParser):

    INDEX_COL = 0

    @classmethod
    def df_from_responses(
        cls, responses: List[dict], use_timestamp: bool
    ) -> pd.DataFrame:
        return super(SwdenParser, cls).df_from_responses(responses)
