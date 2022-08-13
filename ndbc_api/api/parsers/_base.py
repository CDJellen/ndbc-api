from io import StringIO
from datetime import datetime
from typing import List, Tuple

import pandas as pd


class BaseParser:

    HEADER_PREFIX = '#'
    NAN_VALUES = ['MM']
    DATE_PARSER = lambda x: datetime.strptime(x, '%Y %m %d %H %M')
    PARSE_DATES = ['YY', 'MM', 'DD', 'hh', 'mm']
    INDEX_COL = None
    REVERT_COL_NAMES = []

    @classmethod
    def df_from_responses(
        cls, responses: List[dict], use_timestamp: bool = True
    ) -> pd.DataFrame:
        components = []
        for response in responses:
            if response.get('status') == 200:
                components.append(
                    cls._read_response(response, use_timestamp=use_timestamp)
                )
        return pd.concat(components)

    @classmethod
    def _read_response(cls, response: dict, use_timestamp: bool) -> pd.DataFrame:
        body = response.get('body')
        header, data = cls._parse_body(body)
        names = cls._parse_header(header)
        # check that parsed names match parsed values or revert
        if len([v.strip() for v in data[0].split(' ') if v]) != len(names):
            names = cls.REVERT_COL_NAMES
        if use_timestamp:
            parse_dates = {'timestamp': cls.PARSE_DATES}
        else:
            parse_dates = False
        try:
            df = pd.read_csv(
                StringIO('\n'.join(data)),
                names=names,
                delim_whitespace=True,
                na_values=cls.NAN_VALUES,
                parse_dates=parse_dates,
                date_parser=cls.DATE_PARSER,
                index_col=cls.INDEX_COL,
            )
        except (NotImplementedError, TypeError, ValueError):
            return pd.DataFrame()

        return df

    @staticmethod
    def _parse_body(body: str) -> Tuple[List[str], List[str]]:
        buf = StringIO(body)
        data = []
        header = []

        line = buf.readline()
        while line:
            if line.startswith('#'):
                header.append(line)
            else:
                data.append(line)
            line = buf.readline()

        return header, data

    @staticmethod
    def _parse_header(header: List[str]) -> List[str]:
        names = [n for n in header[0].strip('#').strip('\n').split(' ') if n]
        return names or None  # pass 'None' to pd.read_csv on error
