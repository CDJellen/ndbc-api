from ndbc_api.api.parsers.http._base import BaseParser


class SpecParser(BaseParser):

    NAN_VALUES = ['N/A', 'MM', 99.0, 999, 999.0]
    COLUMN_NAN_VALUES = {
        'MWD': {'N/A', 'MM', 999, 999.0},
    }

