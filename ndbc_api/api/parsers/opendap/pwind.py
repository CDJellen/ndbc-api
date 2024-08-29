from typing import List

import xarray as xr

from ndbc_api.api.parsers.opendap._base import BaseParser


class PwindParser(BaseParser):

    INDEX_COL = 0

    @classmethod
    def xr_from_responses(cls, responses: List[dict]) -> xr.Dataset:
        return super(PwindParser,
                     cls).xr_from_responses(responses)
