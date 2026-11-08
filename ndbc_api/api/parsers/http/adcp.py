from ndbc_api.api.parsers.http._base import BaseParser


class AdcpParser(BaseParser):

    NAN_VALUES = ['MM', 999, 999.0, 9999, 9999.0]
    COLUMN_NAN_VALUES = {
        **{f'DEP{i:02d}': {'MM', 999, 999.0} for i in range(1, 39)},
        **{f'DIR{i:02d}': {'MM', 999, 999.0} for i in range(1, 39)},
        **{f'SPD{i:02d}': {'MM', 999, 999.0} for i in range(1, 39)},
    }
    REVERT_COL_NAMES = [
        'YY',
        'MM',
        'DD',
        'hh',
        'mm',
        *[
            col
            for i in range(1, 39)
            for col in (f'DEP{i:02d}', f'DIR{i:02d}', f'SPD{i:02d}')
        ],
    ]

