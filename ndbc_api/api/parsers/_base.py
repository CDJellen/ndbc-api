from io import StringIO
from datetime import datetime
from typing import List, Tuple

import pandas as pd


class BaseParser():

    HEADER_PREFIX = '#'
    NAN_VALUES = ['MM']
    DATE_PARSER = lambda x: lambda x: datetime.strptime(x, '%Y %m %d %H %M')
    PARSE_DATES = ['YY', 'MM', 'DD', 'hh', 'mm']
    INDEX_COL = None

    @classmethod
    def df_from_responses(cls, responses: List[dict]) -> pd.DataFrame:
        components = []
        for response in responses:
            components.append(cls._read_response(response))
        return pd.concat(components)

    @classmethod
    def _read_response(cls, response: dict) -> pd.DataFrame:
        if response.get('status') != 200:
            return pd.DataFrame()

        body = response.get('body')
        header, data = cls._parse_body(body)
        names = cls._parse_header(header)

        df = pd.read_csv(
            StringIO(data),
            names=names,
            delim_whitespace=True,
            na_values=cls.NAN_VALUES,
            parse_dates={'timestamp': cls.PARSE_DATES},
            date_parser=cls.DATE_PARSER,
            index_col=cls.INDEX_COL,
        )

        return df

    @staticmethod
    def _parse_body(body: str) -> Tuple(List[str], List[str]):
        buf = StringIO(body)
        data = []
        header = []

        while (line := buf.readline()):
            if line.startswith('#'):
                header.append(line)
            else:
                data.append(line)

        return header, data

    @staticmethod
    def _parse_header(header: List[str]) -> List[str]:
        names = [n for n in header[0].strip('#').strip('\n').split(' ') if n]
        return names or None  # pass 'None' to pd.read_csv on error
