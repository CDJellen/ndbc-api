from ndbc_api.api.parsers.http._base import BaseParser


class CwindParser(BaseParser):

    NAN_VALUES = [99.0, 999, 9999, 9999.0, 'MM']
    COLUMN_NAN_VALUES = {
        'WDIR': {'MM', 999, 999.0},
        'GDR': {'MM', 999, 999.0},
    }

