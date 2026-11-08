from ndbc_api.api.parsers.http._base import BaseParser


class OceanParser(BaseParser):

    NAN_VALUES = [99.0, 99.9, 999, 9999, 9999.0, 'MM']
    COLUMN_NAN_VALUES = {
        'DEPTH': {'MM', 999, 999.0},
        'OTMP': {'MM', 999, 999.0},
        'COND': {'MM', 999, 999.0},
        'O2%': {'MM', 999, 999.0},
        'TURB': {'MM', 999, 999.0},
        'EH': {'MM', 999, 999.0, 9999, 9999.0},
    }

