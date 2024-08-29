from typing import List

import xarray as xr

from ndbc_api.api.parsers.opendap._base import BaseParser


class StdmetParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = ['MM', 99.0, 999, 9999, 9999.0]

    @classmethod
    def xr_from_responses(cls, responses: List[dict]
                          ) -> xr.Dataset:
        return super(StdmetParser,
                     cls).xr_from_responses(responses)
