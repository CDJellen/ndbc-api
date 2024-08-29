from typing import List

import pandas as pd

from ndbc_api.api.parsers.opendap._base import BaseParser


class DartParser(BaseParser):

    INDEX_COL = 0

    @classmethod
    def xr_from_responses(cls, responses: List[dict],
                          ) -> pd.DataFrame:
        return super(DartParser,
                     cls).xr_from_responses(responses)
