from datetime import datetime
from io import StringIO
from typing import List, Tuple

from ndbc_api.exceptions import ParserException


try:
    import pandas as pd
except ImportError:
    pd = None


class BaseParser:

    HEADER_PREFIX = '#'
    NAN_VALUES = ['MM']
    DATE_PARSER = '%Y %m %d %H %M'
    PARSE_DATES = [0, 1, 2, 3, 4]
    INDEX_COL = False
    REVERT_COL_NAMES = []

    @classmethod
    def parse_responses(cls,
                        responses: List[dict],
                        use_timestamp: bool = True) -> List[dict]:
        components = []
        for response in responses:
            if response.get('status') == 200:
                components.extend(
                    cls._read_response_fallback(response, use_timestamp=use_timestamp))
        if use_timestamp:
            # drop duplicates by timestamp, keeping first
            seen = set()
            unique_components = []
            for row in components:
                ts = row.get('timestamp')
                if ts not in seen:
                    seen.add(ts)
                    unique_components.append(row)
            # sort by timestamp
            unique_components.sort(key=lambda x: x.get('timestamp') or datetime.min)
            components = unique_components
        return components


    @classmethod
    def _read_response_fallback(cls, response: dict,
                                use_timestamp: bool) -> List[dict]:
        body = response.get('body')
        header, data = cls._parse_body(body)
        names = cls._parse_header(header)
        if not data or not names:
            return []
        # check that parsed names match parsed values or revert
        if len([v.strip() for v in data[0].strip('\n').split(' ') if v
               ]) != len(names):
            names = cls.REVERT_COL_NAMES
        if '(' in data[0]:
            data = cls._clean_data(data)
        if not data:
            return []

        rows = []
        for line in data:
            parts = [v.strip() for v in line.strip('\n').split(' ') if v]
            if not parts:
                continue
            row = {}
            for name, val in zip(names, parts):
                # Check string representation first
                if cls.NAN_VALUES and val in cls.NAN_VALUES:
                    row[name] = None
                    continue
                
                # Convert to numeric if possible
                try:
                    parsed_val = float(val) if '.' in val else int(val)
                except ValueError:
                    parsed_val = val
                
                # Check numeric representation
                if cls.NAN_VALUES and parsed_val in cls.NAN_VALUES:
                    row[name] = None
                else:
                    row[name] = parsed_val

            if use_timestamp:
                date_col_names = [names[i] for i in cls.PARSE_DATES if i < len(names)]
                date_parts = [str(row.get(col, '')) for col in date_col_names]
                date_str = ' '.join(date_parts)
                try:
                    row['timestamp'] = datetime.strptime(date_str, cls.DATE_PARSER)
                except ValueError:
                    row['timestamp'] = None
                for col in date_col_names:
                    row.pop(col, None)
            rows.append(row)
        return rows

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
