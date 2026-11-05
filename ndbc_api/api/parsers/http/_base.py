from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Set, Tuple

from ndbc_api.exceptions import ParserException


try:
    import pandas as pd
except ImportError:
    pd = None


class BaseParser:

    HEADER_PREFIX = '#'
    NAN_VALUES = ['MM']
    COLUMN_NAN_VALUES: Dict[str, Set[Any]] = {}
    DATE_PARSER = '%Y %m %d %H %M'
    PARSE_DATES = [0, 1, 2, 3, 4]
    INDEX_COL = False
    REVERT_COL_NAMES = []

    @classmethod
    def parse_responses(cls,
                        responses: List[dict],
                        use_timestamp: bool = True) -> List[dict]:
        components = []
        all_cols: List[str] = []
        seen_cols: Set[str] = set()
        for response in responses:
            if response.get('status') == 200:
                rows = cls._read_response(response, use_timestamp=use_timestamp)
                if rows:
                    for k in rows[0].keys():
                        if k not in seen_cols:
                            seen_cols.add(k)
                            all_cols.append(k)
                    components.extend(rows)
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
        if len(all_cols) > 0 and components:
            for row in components:
                if len(row) < len(all_cols):
                    for col in all_cols:
                        if col not in row:
                            row[col] = None
        return components

    @classmethod
    def df_from_responses(cls,
                          responses: List[dict],
                          use_timestamp: bool = True) -> 'pd.DataFrame':
        """Parse responses and return a `pandas.DataFrame` (backward-compatible helper)."""
        if pd is None:
            raise ImportError(
                'Pandas is not installed. Please install it using `pip install pandas`.'
            )
        rows = cls.parse_responses(responses=responses, use_timestamp=use_timestamp)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        if use_timestamp:
            if 'timestamp' not in df.columns:
                raise ParserException('Missing `timestamp` column in parsed data.')
            df = df.set_index('timestamp').sort_index()
        return df.where(df.notna())

    """ PRIVATE """

    @classmethod
    def _build_col_nan_sets(cls, col_name: str) -> Tuple[Set[str], Set[int], Set[float]]:
        """Precompute O(1) string, int, and float sentinel sets for `col_name`.

        Distinguishes integer vs. float sentinels so valid integer measurements
        (e.g., wind/wave direction `99` degrees) are not coerced to `None` by
        float sentinels (`99.0`), and valid storm pressures (`999.0` hPa in `PRES`)
        are not coerced by integer sentinels (`999`).
        """
        source = (
            cls.COLUMN_NAN_VALUES.get(col_name)
            if cls.COLUMN_NAN_VALUES and col_name in cls.COLUMN_NAN_VALUES
            else cls.NAN_VALUES
        )
        str_nans: Set[str] = set()
        int_nans: Set[int] = set()
        float_nans: Set[float] = set()
        if not source:
            return str_nans, int_nans, float_nans
        for s in source:
            if isinstance(s, str):
                str_nans.add(s)
            elif isinstance(s, int) and not isinstance(s, bool):
                int_nans.add(s)
                float_nans.add(float(s))
            elif isinstance(s, float):
                float_nans.add(s)
        return str_nans, int_nans, float_nans

    @classmethod
    def _is_nan(cls, col_name: str, raw_val: str, parsed_val: Any = None) -> bool:
        """Determine whether a token represents a missing measurement sentinel."""
        str_nans, int_nans, float_nans = cls._build_col_nan_sets(col_name)
        if raw_val in str_nans:
            return True
        if parsed_val is not None:
            if isinstance(parsed_val, int) and not isinstance(parsed_val, bool):
                return parsed_val in int_nans
            if isinstance(parsed_val, float):
                return parsed_val in float_nans
        return False

    @classmethod
    def _resolve_date_indices(cls, names: List[str]) -> List[int]:
        """Resolve date column indices, supporting both 5-col (`YY MM DD hh mm`)
        and legacy 4-col (`YY MM DD hh`, pre-2005) NDBC headers."""
        if cls.PARSE_DATES != [0, 1, 2, 3, 4]:
            return [i for i in cls.PARSE_DATES if i < len(names)]
        if (
            len(names) >= 5
            and names[3].lower() in ('hh', 'hr')
            and names[4].lower() not in ('mm', 'mn')
        ):
            return [0, 1, 2, 3]
        return [i for i in cls.PARSE_DATES if i < len(names)]

    @classmethod
    def _read_response(cls, response: dict,
                       use_timestamp: bool) -> List[dict]:
        body = response.get('body')
        header, data = cls._parse_body(body)
        names = cls._parse_header(header)
        if not data or not names:
            return []
        # check that parsed names match parsed values or revert
        if len(data[0].split()) != len(names):
            names = cls.REVERT_COL_NAMES
        if '(' in data[0]:
            data = cls._clean_data(data)
        if not data or not names:
            return []

        date_indices = cls._resolve_date_indices(names) if use_timestamp else []
        date_col_names = [names[i] for i in date_indices]
        date_col_set = set(date_col_names)
        use_fast_dt = (
            use_timestamp
            and cls.DATE_PARSER == '%Y %m %d %H %M'
            and len(date_col_names) in (4, 5)
        )

        # Precompute column specs once per response for tight inner loop
        col_specs = []
        for name in names:
            is_date = name in date_col_set
            if is_date:
                col_specs.append((name, True, None, None, None))
            else:
                s_nans, i_nans, f_nans = cls._build_col_nan_sets(name)
                col_specs.append((name, False, s_nans, i_nans, f_nans))

        has_5_dates = len(date_col_names) == 5
        d0 = date_col_names[0] if use_fast_dt else ''
        d1 = date_col_names[1] if use_fast_dt else ''
        d2 = date_col_names[2] if use_fast_dt else ''
        d3 = date_col_names[3] if use_fast_dt else ''
        d4 = date_col_names[4] if (use_fast_dt and has_5_dates) else ''

        rows = []
        for line in data:
            parts = line.split()
            if not parts:
                continue
            row = {}
            for (name, is_date, s_nans, i_nans, f_nans), val in zip(col_specs, parts):
                if not is_date and val in s_nans:
                    row[name] = None
                    continue
                if '.' in val:
                    try:
                        fval = float(val)
                        row[name] = None if (not is_date and fval in f_nans) else fval
                    except ValueError:
                        row[name] = val
                else:
                    try:
                        ival = int(val)
                        row[name] = None if (not is_date and ival in i_nans) else ival
                    except ValueError:
                        row[name] = val

            if use_timestamp:
                if use_fast_dt:
                    try:
                        yr = int(row.get(d0))
                        if 0 <= yr < 100:
                            yr = (1900 + yr) if yr >= 70 else (2000 + yr)
                        mo = int(row.get(d1))
                        dy = int(row.get(d2))
                        hr = int(row.get(d3))
                        mn = int(row.get(d4, 0)) if has_5_dates else 0
                        row['timestamp'] = datetime(yr, mo, dy, hr, mn)
                    except (ValueError, TypeError):
                        row['timestamp'] = None
                else:
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

    _read_response_fallback = _read_response

    @staticmethod
    def _parse_body(body: str) -> Tuple[List[str], List[str]]:
        buf = StringIO(body)
        data = []
        header = []

        line = buf.readline()
        while line:
            stripped_upper = line.lstrip().upper()
            if line.startswith('#') or (
                not data and stripped_upper.startswith(('YY ', 'YYYY ', 'YR ', 'MO '))
            ):
                header.append(line)
            else:
                data.append(line)
            line = buf.readline()

        return header, data

    @staticmethod
    def _parse_header(header: List[str]) -> List[str]:
        names = (header[0].lstrip('#').split()
                 if isinstance(header, list) and len(header) > 0 else None)
        return names

    @staticmethod
    def _clean_data(data: List[str]) -> List[str]:
        vals = [
            ' '.join([v
                      for v in r.split(' ')
                      if v and '(' not in v])
            for r in data
        ]
        return vals or None
