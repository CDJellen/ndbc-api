from typing import List

import pandas as pd

from api.parsers._html import HtmlParser


class StationMetaParser(HtmlParser):

    INDEX_COL = None  # TODO

    @classmethod
    def df_from_responses(cls, responses: List[dict]) -> pd.DataFrame:
        dfs = super(StationMetaParser, cls).dfs_from_responses(responses)
        df = pd.DataFrame()  # TODO
        return df
