from typing import List

import pandas as pd

from ndbc_api.api.parsers._base import BaseParser


class SwdenParser(BaseParser):

    INDEX_COL = 0
    NAN_VALUES = [99.0, 999, 999.0, 9999, 9999.0, 'MM']
    REVERT_COL_NAMES = [
        'YY',
        'MM',
        'DD',
        'hh',
        'mm',
        '.0200',
        '.0325',
        '.0375',
        '.0425',
        '.0475',
        '.0525',
        '.0575',
        '.0625',
        '.0675',
        '.0725',
        '.0775',
        '.0825',
        '.0875',
        '.0925',
        '.1000',
        '.1100',
        '.1200',
        '.1300',
        '.1400',
        '.1500',
        '.1600',
        '.1700',
        '.1800',
        '.1900',
        '.2000',
        '.2100',
        '.2200',
        '.2300',
        '.2400',
        '.2500',
        '.2600',
        '.2700',
        '.2800',
        '.2900',
        '.3000',
        '.3100',
        '.3200',
        '.3300',
        '.3400',
        '.3500',
        '.3650',
        '.3850',
        '.4050',
        '.4250',
        '.4450',
        '.4650',
        '.4850',
    ]

    @classmethod
    def df_from_responses(
        cls, responses: List[dict], use_timestamp: bool
    ) -> pd.DataFrame:
        return super(SwdenParser, cls).df_from_responses(responses)
