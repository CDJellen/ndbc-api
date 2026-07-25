from typing import List

from ndbc_api.api.parsers.http._base import BaseParser


class SuplParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = [99.0, 999, 9999, 9999.0, 'MM']

