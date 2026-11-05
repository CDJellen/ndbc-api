from ndbc_api.api.parsers.http._base import BaseParser


class StdmetParser(BaseParser):

    NAN_VALUES = ['MM', 99.0, 999, 9999, 9999.0]
    COLUMN_NAN_VALUES = {
        'PRES': {'MM', 9999, 9999.0},
        'BAR': {'MM', 9999, 9999.0},
        'WDIR': {'MM', 999, 999.0},
        'WD': {'MM', 999, 999.0},
        'MWD': {'MM', 999, 999.0},
    }

