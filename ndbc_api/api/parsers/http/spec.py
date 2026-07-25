from typing import List

from ndbc_api.api.parsers.http._base import BaseParser


class SpecParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = ['N/A']

