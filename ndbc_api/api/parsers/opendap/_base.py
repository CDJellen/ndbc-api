from typing import List

import xarray as xr

from ndbc_api.exceptions import ParserException


class BaseParser:

    HEADER_PREFIX = '#'
    NAN_VALUES = ['MM']
    DATE_PARSER = '%Y %m %d %H %M'
    PARSE_DATES = ['YY', 'MM', 'DD', 'hh', 'mm']
    INDEX_COL = False
    REVERT_COL_NAMES = []

    @classmethod
    def xr_from_responses(cls,
                          responses: List[dict]
                          ) -> xr.Dataset:
        """"""
        pass
