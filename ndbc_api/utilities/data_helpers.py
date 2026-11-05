"""Shared pure-function helpers for data handling.

These functions are used by both `NdbcApi` and `AsyncNdbcApi`. They are
intentionally stateless — no `self`, no I/O — so that they can be imported
without pulling either API class into scope.
"""
from datetime import date, datetime, timezone
import re
from typing import Any, Dict, List, Optional, Sequence, Union

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import polars as pl
except ImportError:
    pl = None

try:
    import xarray
except ImportError:
    xarray = None

from ..exceptions import (
    HandlerException,
    ParserException,
    TimestampException,
)
from .mode_metadata import COLUMN_UNITS
from .opendap.dataset import merge_datasets


_REALTIME_EXT_TO_MODE: Dict[str, str] = {
    'txt': 'stdmet',
    'cwind': 'cwind',
    'spec': 'spec',
    'data_spec': 'swden',
    'swden': 'swden',
    'swdir': 'swdir',
    'swdir2': 'swdir2',
    'swr1': 'swr1',
    'swr2': 'swr2',
    'supl': 'supl',
    'adcp': 'adcp',
    'ocean': 'ocean',
    'srad': 'srad',
    'rain': 'rain',
}

_HIST_ID_TO_MODE: Dict[str, str] = {
    'h': 'stdmet',
    'c': 'cwind',
    'a': 'adcp',
    'o': 'ocean',
    's': 'supl',
    'w': 'swden',
    'd': 'swdir',
    'i': 'swdir2',
    'j': 'swr1',
    'k': 'swr2',
}

_HIST_TITLE_TO_MODE: List[tuple] = [
    ('alpha1', 'swdir'),
    ('alpha2', 'swdir2'),
    ('(r1)', 'swr1'),
    ('(r2)', 'swr2'),
    ('spectral wave', 'swden'),
    ('standard meteorological', 'stdmet'),
    ('continuous winds', 'cwind'),
    ('supplemental', 'supl'),
    ('oceanographic', 'ocean'),
    ('acoustic doppler', 'adcp'),
    ('adcp', 'adcp'),
]


def extract_realtime_modes(
    full_data: Dict[str, Any],
    supported_modes: Sequence[str],
) -> List[str]:
    """Extract canonical NDBC mode names from parsed `available_realtime` links."""
    if not isinstance(full_data, dict):
        return []
    supported_set = set(supported_modes)
    station_modes = set()

    for _, entry in full_data.items():
        if not isinstance(entry, dict):
            continue
        matched_from_url = False
        for key, url in entry.items():
            if not isinstance(url, str):
                continue
            if key != 'description' and ('/realtime2/' in url or '.' in url.rsplit('/', 1)[-1]):
                clean_url = url.split('?', 1)[0].split('#', 1)[0]
                ext = clean_url.rsplit('.', 1)[-1].lower()
                mapped = _REALTIME_EXT_TO_MODE.get(ext)
                if mapped and mapped in supported_set:
                    station_modes.add(mapped)
                    matched_from_url = True

        desc = entry.get('description')
        if isinstance(desc, str):
            if '#' in desc:
                frag = desc.rsplit('#', 1)[-1].strip().lower()
                if frag in supported_set:
                    station_modes.add(frag)
                    matched_from_url = True
            if not matched_from_url:
                for m in supported_set:
                    if m == 'spec' and 'data_spec' in desc:
                        continue
                    if m in desc:
                        station_modes.add(m)

    return sorted(station_modes)


def summarize_historical_modes(
    historical_data: Dict[str, Any],
    supported_modes: Optional[Sequence[str]] = None,
) -> Dict[str, List[str]]:
    """Map human-readable historical measurement sections to canonical NDBC modes and periods."""
    if not isinstance(historical_data, dict):
        return {}
    supported_set = set(supported_modes) if supported_modes is not None else None
    summary: Dict[str, List[str]] = {}
    dir_pattern = re.compile(r'(?:dir=|/)data/(?:historical/)?([a-z0-9_]+)/', re.IGNORECASE)
    file_pattern = re.compile(r'filename=[a-z0-9]+([hcaoswdijk])\d{4}\.txt', re.IGNORECASE)

    for section_title, links in historical_data.items():
        if not isinstance(links, dict):
            continue
        detected_mode = None
        for _, url in links.items():
            if not isinstance(url, str):
                continue
            m_dir = dir_pattern.search(url)
            if m_dir:
                candidate = m_dir.group(1).lower()
                if supported_set is None or candidate in supported_set:
                    detected_mode = candidate
                    break
            m_file = file_pattern.search(url)
            if m_file:
                candidate = _HIST_ID_TO_MODE.get(m_file.group(1).lower())
                if candidate and (supported_set is None or candidate in supported_set):
                    detected_mode = candidate
                    break

        if detected_mode is None and isinstance(section_title, str):
            low_title = section_title.lower()
            for substr, mode_name in _HIST_TITLE_TO_MODE:
                if substr in low_title and (supported_set is None or mode_name in supported_set):
                    detected_mode = mode_name
                    break

        if detected_mode:
            periods = [str(k) for k in links.keys()]
            if detected_mode not in summary:
                summary[detected_mode] = periods
            else:
                for p in periods:
                    if p not in summary[detected_mode]:
                        summary[detected_mode].append(p)

    return summary


def _attach_units(df: Any) -> Any:
    """Attach NDBC column unit metadata to `df.attrs['units']` if applicable."""
    if pd is not None and isinstance(df, pd.DataFrame) and len(df.columns) > 0:
        units = {
            str(col): COLUMN_UNITS[str(col)]
            for col in df.columns
            if str(col) in COLUMN_UNITS
        }
        if units:
            df.attrs['units'] = units
    return df


def parse_station_id(station_id: Union[str, int]) -> str:
    """Normalise a station identifier to a lowercase string."""
    return str(station_id).lower()


def filter_station_deployments(
    records: Any,
    station_id: Union[str, int],
    start_time: Any = None,
    end_time: Any = None,
) -> List[dict]:
    """Filter historical deployment records to a station and optional time window."""
    if hasattr(records, 'to_dict'):
        records = records.to_dict(orient='records')
    elif hasattr(records, 'to_dicts'):
        records = records.to_dicts()

    target_sid = parse_station_id(station_id).strip()
    start_dt = handle_timestamp(start_time) if start_time is not None else None
    end_dt = handle_timestamp(end_time) if end_time is not None else None
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise TimestampException(
            f'`start_time` ({start_dt}) must be <= `end_time` ({end_dt}).'
        )

    matched = []
    for row in records:
        if not isinstance(row, dict):
            continue
        row_sid = str(row.get('Station') or '').strip().lower()
        if row_sid != target_sid:
            continue

        if start_dt is not None or end_dt is not None:
            raw_start = row.get('Start Date')
            raw_end = row.get('End Date')
            try:
                dep_start = (
                    handle_timestamp(raw_start)
                    if raw_start and str(raw_start).strip()
                    else datetime.min
                )
            except TimestampException:
                dep_start = datetime.min
            try:
                dep_end = (
                    handle_timestamp(raw_end)
                    if raw_end and str(raw_end).strip()
                    else datetime.max
                )
            except TimestampException:
                dep_end = datetime.max

            if start_dt is not None and dep_end < start_dt:
                continue
            if end_dt is not None and dep_start > end_dt:
                continue

        matched.append(row.copy())

    matched.sort(key=lambda r: (str(r.get('Start Date') or ''), str(r.get('End Date') or '')))
    return matched


def handle_timestamp(timestamp: Union[datetime, date, str, Any]) -> datetime:
    """Convert `timestamp` to a naive UTC `datetime.datetime`.

    Supports `datetime.datetime` (naive or timezone-aware), `datetime.date`,
    `pandas.Timestamp`, `'%Y-%m-%d'` strings, and ISO-8601 timestamp strings
    (e.g. `'2022-01-15T06:00:00Z'`).

    Raises:
        TimestampException: If `timestamp` cannot be parsed.
    """
    if pd is not None and (isinstance(timestamp, pd.Timestamp) or timestamp is pd.NaT):
        if pd.isna(timestamp):
            raise TimestampException('Timestamp cannot be NaT.')
        dt = timestamp.to_pydatetime()
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is not None:
            return timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        return timestamp

    if isinstance(timestamp, date):
        return datetime(timestamp.year, timestamp.month, timestamp.day)

    if not isinstance(timestamp, str):
        raise TimestampException(
            f'Unsupported timestamp type: {type(timestamp).__name__}'
        )

    s = timestamp.strip()
    try:
        return datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        pass

    try:
        iso_str = s[:-1] + '+00:00' if s.endswith(('Z', 'z')) else s
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError as e:
        raise TimestampException(
            f"Cannot parse timestamp {timestamp!r}. Expected '%Y-%m-%d' or ISO-8601 format."
        ) from e


def enforce_timerange(
    df: Union[List[dict], Any],
    start_time: datetime,
    end_time: datetime,
) -> Union[List[dict], Any]:
    """Down-select `df` to rows within `[start_time, end_time]`.

    Raises:
        TimestampException: If the index slice fails or `start_time > end_time`.
    """
    if isinstance(start_time, datetime) and isinstance(end_time, datetime):
        if start_time > end_time:
            raise TimestampException(
                f'`start_time` ({start_time}) must be <= `end_time` ({end_time}).'
            )

    if isinstance(df, list):
        filtered = []
        for row in df:
            ts = row.get('timestamp')
            if ts is not None:
                if start_time <= ts <= end_time:
                    filtered.append(row)
        return filtered

    if pd is not None and isinstance(df, pd.DataFrame):
        try:
            df = df.loc[(df.index.values >= pd.Timestamp(start_time)) &
                        (df.index.values <= pd.Timestamp(end_time))]
        except ValueError as e:
            raise TimestampException(
                'Failed to enforce `start_time` to `end_time` range.') from e
        return df

    if pl is not None and isinstance(df, pl.DataFrame):
        if 'timestamp' in df.columns:
            return df.filter((pl.col('timestamp') >= start_time) & (pl.col('timestamp') <= end_time))
        return df

    return df


def handle_data(
    data: Any,
    as_df: bool = True,
    as_pl: bool = False,
    cols: Optional[List[str]] = None,
) -> Any:
    """Apply optional column selection and return-format conversion.

    Raises:
        ParserException: If column selection fails.
        HandlerException: If dict → DataFrame conversion fails.
    """
    if cols:
        try:
            if isinstance(data, list):
                if data and cols:
                    available_cols = {k for row in data for k in row.keys()}
                    for col in cols:
                        if col not in available_cols:
                            raise KeyError(f"Column '{col}' not found in data.")
                new_data = []
                for row in data:
                    new_row = {col: row.get(col) for col in cols}
                    if 'timestamp' in row:
                        new_row['timestamp'] = row['timestamp']
                    if 'station_id' in row:
                        new_row['station_id'] = row['station_id']
                    new_data.append(new_row)
                data = new_data
            elif isinstance(data, dict):
                new_data = {}
                for k, v in data.items():
                    if isinstance(v, dict):
                        new_data[k] = {col: val for col, val in v.items() if col in cols}
                    elif k in cols:
                        new_data[k] = v
                data = new_data
            elif pd is not None and isinstance(data, pd.DataFrame):
                data = data[[*cols]]
            elif pl is not None and isinstance(data, pl.DataFrame):
                data = data.select([col for col in cols if col in data.columns])
        except (KeyError, ValueError) as e:
            raise ParserException(
                'Failed to parse column selection.') from e

    if as_pl:
        if pl is None:
            raise ImportError('Polars is not installed. Please install it using `pip install polars`.')
        if isinstance(data, list):
            return pl.DataFrame(data, infer_schema_length=None)
        elif isinstance(data, dict):
            rows = []
            for k, v in data.items():
                if isinstance(v, dict):
                    row = {'index': k}
                    row.update(v)
                    rows.append(row)
                else:
                    rows.append({'index': k, 'value': v})
            return pl.DataFrame(rows, infer_schema_length=None)
        elif pd is not None and isinstance(data, pd.DataFrame):
            return pl.from_pandas(data)
        elif isinstance(data, pl.DataFrame):
            return data
        else:
            try:
                return pl.DataFrame(data, infer_schema_length=None)
            except Exception as e:
                raise HandlerException('Failed to convert data to pl.DataFrame.') from e

    if as_df:
        if pd is None:
            raise ImportError('Pandas is not installed. Please install it using `pip install pandas`.')
        if isinstance(data, list):
            return _attach_units(pd.DataFrame(data))
        elif isinstance(data, dict):
            try:
                if not data:
                    return pd.DataFrame()
                if all(not isinstance(v, (dict, list, tuple, set)) for v in data.values()):
                    return _attach_units(pd.DataFrame([data]))
                return _attach_units(pd.DataFrame().from_dict(data, orient='index'))
            except (NotImplementedError, ValueError, TypeError) as e:
                raise HandlerException(
                    'Failed to convert `pd.DataFrame` to `dict`.') from e
        elif isinstance(data, pd.DataFrame):
            return _attach_units(data)
        else:
            try:
                return _attach_units(pd.DataFrame(data))
            except Exception as e:
                raise HandlerException('Failed to convert data to pd.DataFrame.') from e

    if not as_df and not as_pl:
        if pd is not None and isinstance(data, pd.DataFrame):
            return data.to_dict()
        elif pl is not None and isinstance(data, pl.DataFrame):
            return data.to_dict(as_series=False)

    return data


def handle_accumulate_data(
    accumulated_data: Dict[str, List[Any]],
    as_df: bool = True,
    as_pl: bool = False,
    as_xarray_dataset: bool = False,
) -> Any:
    """Coalesce data from multiple stations and modes."""
    # Prune any modalities that returned no data
    for k in list(accumulated_data.keys()):
        if not accumulated_data[k]:
            del accumulated_data[k]

    if not accumulated_data:
        if as_xarray_dataset:
            if xarray is None:
                raise ImportError(
                    'xarray is required for OpenDAP support. If you uninstalled it '
                    'to create a lightweight environment, you must reinstall it to use this feature.'
                )
            return xarray.Dataset()
        return {}

    # Determine return type from the first available data item
    first_key = list(accumulated_data.keys())[0]
    first_item = accumulated_data[first_key][0]

    use_opendap = (xarray is not None and isinstance(first_item, xarray.Dataset)) or as_xarray_dataset

    if use_opendap:
        data_list = []
        for mode, station_data in accumulated_data.items():
            data_list.extend(station_data)
        return merge_datasets(data_list)

    if isinstance(first_item, dict):
        return accumulated_data

    # Funnel all data through the List[dict] IR
    raw_list = []
    for mode, station_data in accumulated_data.items():
        for item in station_data:
            if pd is not None and isinstance(item, pd.DataFrame):
                raw_list.extend(item.reset_index().to_dict(orient='records'))
            elif pl is not None and isinstance(item, pl.DataFrame):
                raw_list.extend(item.to_dicts())
            elif isinstance(item, list):
                raw_list.extend(item)
            elif isinstance(item, dict):
                raw_list.append(item)

    if not raw_list:
        if as_pl:
            return pl.DataFrame()
        if as_df:
            return pd.DataFrame()
        return {}

    index_cols = ['timestamp', 'station_id']
    has_index = any(col in row for row in raw_list for col in index_cols)

    if not has_index:
        merged_list = raw_list
    else:
        grouped = {}
        for row in raw_list:
            ts = row.get('timestamp')
            st_id = row.get('station_id')
            key = (ts, st_id)
            if key not in grouped:
                grouped[key] = row.copy()
            else:
                existing = grouped[key]
                for k, v in row.items():
                    if existing.get(k) is None:
                        existing[k] = v
        merged_list = list(grouped.values())

    if as_pl:
        if pl is None:
            raise ImportError('Polars is not installed.')
        df = pl.DataFrame(merged_list, infer_schema_length=None)
        sort_cols = [c for c in ('timestamp', 'station_id') if c in df.columns]
        if sort_cols:
            df = df.sort(sort_cols)
        return df

    if as_df:
        if pd is None:
            raise ImportError('Pandas is not installed.')
        df = pd.DataFrame(merged_list)
        if df.empty:
            return df
        present_index_cols = [
            col for col in index_cols if col in df.columns
        ]
        if present_index_cols:
            df.set_index(present_index_cols, inplace=True)
            df.sort_index(inplace=True)
        return _attach_units(df.where(df.notna()))

    return accumulated_data
