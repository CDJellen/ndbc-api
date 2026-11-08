from ndbc_api.api.parsers.http._base import BaseParser


class SuplParser(BaseParser):

    NAN_VALUES = [99.0, 999, 9999, 9999.0, 'MM']
    COLUMN_NAN_VALUES = {
        'PRES': {'MM', 9999, 9999.0},
        'PRESS': {'MM', 9999, 9999.0},
        'WDIR': {'MM', 999, 999.0},
    }

