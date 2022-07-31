from typing import List

import pandas as pd

from api.parsers._base import BaseParser


class Swr1Parser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = [99.0, 999, 999.0, 9999]
    VALUE_PARSER = lambda x: float(str(x).strip('(').strip(')'))

    @classmethod
    def df_from_responses(cls, responses: List[dict]) -> pd.DataFrame:
        df = super(Swr1Parser, cls).df_from_responses(responses)
        df = df.applymap(cls.VALUE_PARSER)
        return df
