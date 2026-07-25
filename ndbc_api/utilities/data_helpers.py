"""Shared pure-function helpers for data handling.

These functions are used by both :class:`NdbcApi` and
:class:`AsyncNdbcApi`.  They are intentionally stateless — no ``self``,
no I/O — so that they can be imported without pulling either API class
into scope.
"""
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

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
from .opendap.dataset import merge_datasets


def parse_station_id(station_id: Union[str, int]) -> str:
    """Normalise a station identifier to a lowercase string."""
    return str(station_id).lower()


def handle_timestamp(timestamp: Union[datetime, str]) -> datetime:
    """Convert *timestamp* to :class:`datetime.datetime`.

    Raises:
        TimestampException: If the string cannot be parsed as ``%Y-%m-%d``.
    """
    if isinstance(timestamp, datetime):
        return timestamp
    try:
        return datetime.strptime(timestamp, '%Y-%m-%d')
    except ValueError as e:
        raise TimestampException from e


def enforce_timerange(
    df: Union[List[dict], Any],
    start_time: datetime,
    end_time: datetime,
) -> Union[List[dict], Any]:
    """Down-select *df* to rows within [*start_time*, *end_time*].

    Raises:
        TimestampException: If the index slice fails.
    """
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
                    first_row = data[0]
                    for col in cols:
                        if col not in first_row:
                            raise KeyError(f"Column '{col}' not found in data.")
                new_data = []
                preserve_cols = set(cols) | {'timestamp', 'station_id'}
                for row in data:
                    new_row = {k: v for k, v in row.items() if k in preserve_cols}
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
            raise ImportError("Polars is not installed. Please install it using `pip install polars`.")
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
            raise ImportError("Pandas is not installed. Please install it using `pip install pandas`.")
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            try:
                return pd.DataFrame().from_dict(data, orient='index')
            except (NotImplementedError, ValueError, TypeError) as e:
                raise HandlerException(
                    'Failed to convert `pd.DataFrame` to `dict`.') from e
        elif isinstance(data, pd.DataFrame):
            return data
        else:
            try:
                return pd.DataFrame(data)
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
                raise ImportError("xarray is required for OpenDAP support. If you uninstalled it to create a lightweight environment, you must reinstall it to use this feature.")
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
            raise ImportError("Polars is not installed.")
        df = pl.DataFrame(merged_list, infer_schema_length=None)
        if 'timestamp' in df.columns:
            df = df.sort('timestamp')
        return df

    if as_df:
        if pd is None:
            raise ImportError("Pandas is not installed.")
        df = pd.DataFrame(merged_list)
        if df.empty:
            return df
        present_index_cols = [
            col for col in index_cols if col in df.columns
        ]
        if present_index_cols:
            df.set_index(present_index_cols, inplace=True)
            df.sort_index(inplace=True)
        return df.where(df.notna())

    return accumulated_data
