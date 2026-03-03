from io import StringIO
from typing import List, Tuple

import pandas as pd

from ndbc_api.exceptions import ParserException


class BaseParser:

    HEADER_PREFIX = '#'
    NAN_VALUES = ['MM']
    DATE_PARSER = '%Y %m %d %H %M'
    PARSE_DATES = [0, 1, 2, 3, 4]
    INDEX_COL = False
    REVERT_COL_NAMES = []

    @classmethod
    def df_from_responses(cls,
                          responses: List[dict],
                          use_timestamp: bool = True) -> pd.DataFrame:
        components = []
        for response in responses:
            if response.get('status') == 200:
                components.append(
                    cls._read_response(response, use_timestamp=use_timestamp))
        df = pd.concat(components)
        if use_timestamp:
            try:
                df = df.reset_index().drop_duplicates(subset='timestamp',
                                                      keep='first')
                df = df.set_index('timestamp').sort_index()
            except KeyError as e:
                raise ParserException from e
        # Normalize null representation: concat may introduce
        # None when aligning DataFrames with different columns.
        return df.where(df.notna())

    @classmethod
    def _read_response(cls, response: dict,
                       use_timestamp: bool) -> pd.DataFrame:
        body = response.get('body')
        header, data = cls._parse_body(body)
        names = cls._parse_header(header)
        if not data:
            return pd.DataFrame()
        # check that parsed names match parsed values or revert
        if len([v.strip() for v in data[0].strip('\n').split(' ') if v
               ]) != len(names):
            names = cls.REVERT_COL_NAMES
        if '(' in data[0]:
            data = cls._clean_data(data)

        try:
            df = pd.read_csv(
                StringIO('\n'.join(data)),
                names=names,
                sep=r'\s+',
                na_values=cls.NAN_VALUES,
                index_col=cls.INDEX_COL,
            )
            if use_timestamp:
                # Reset index so date columns are accessible as
                # regular columns (INDEX_COL=0 absorbs column 0).
                df = df.reset_index()
                date_col_names = [names[i] for i in cls.PARSE_DATES]
                date_strings = (
                    df[date_col_names].astype(str).agg(' '.join, axis=1))
                df['timestamp'] = pd.to_datetime(
                    date_strings, format=cls.DATE_PARSER)
                df = df.drop(columns=date_col_names)
                df = df.set_index('timestamp')

        except (NotImplementedError, TypeError, ValueError) as e:
            print(e)
            return pd.DataFrame()

        # Normalize null representation: read_csv may produce
        # None for object-dtype columns; standardize to NaN.
        return df.where(df.notna())

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
        names = ([n for n in header[0].strip('#').strip('\n').split(' ') if n]
                 if isinstance(header, list) and len(header) > 0 else None)
        return names  # pass 'None' to pd.read_csv on error

    @staticmethod
    def _clean_data(data: List[str]) -> List[str]:
        vals = [
            ' '.join([v
                      for v in r.split(' ')
                      if v and '(' not in v])
            for r in data
        ]
        return vals or None  # pass 'None' to pd.read_csv on error
