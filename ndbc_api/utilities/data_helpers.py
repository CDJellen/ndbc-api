"""Shared pure-function helpers for data handling.

These functions are used by both :class:`NdbcApi` and
:class:`AsyncNdbcApi`.  They are intentionally stateless — no ``self``,
no I/O — so that they can be imported without pulling either API class
into scope.
"""
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import xarray

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
    df: pd.DataFrame,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """Down-select *df* to rows within [*start_time*, *end_time*].

    Raises:
        TimestampException: If the index slice fails.
    """
    try:
        df = df.loc[(df.index.values >= pd.Timestamp(start_time)) &
                    (df.index.values <= pd.Timestamp(end_time))]
    except ValueError as e:
        raise TimestampException(
            'Failed to enforce `start_time` to `end_time` range.') from e
    return df


def handle_data(
    data: pd.DataFrame,
    as_df: bool = True,
    cols: Optional[List[str]] = None,
) -> Union[pd.DataFrame, dict]:
    """Apply optional column selection and return-format conversion.

    Raises:
        ParserException: If column selection fails.
        HandlerException: If dict → DataFrame conversion fails.
    """
    if cols:
        try:
            data = data[[*cols]]
        except (KeyError, ValueError) as e:
            raise ParserException(
                'Failed to parse column selection.') from e
    if as_df and isinstance(data, pd.DataFrame):
        return data
    elif isinstance(data, pd.DataFrame) and not as_df:
        return data.to_dict()
    elif as_df:
        try:
            return pd.DataFrame().from_dict(data, orient='index')
        except (NotImplementedError, ValueError, TypeError) as e:
            raise HandlerException(
                'Failed to convert `pd.DataFrame` to `dict`.') from e
    else:
        return data


def handle_accumulate_data(
    accumulated_data: Dict[str, List[Union[pd.DataFrame, dict,
                                           xarray.Dataset]]],
    as_xarray_dataset: bool = False,
) -> Union[pd.DataFrame, dict, xarray.Dataset]:
    """Coalesce data from multiple stations and modes.

    Accepts the ``accumulated_data`` dict keyed by mode name, where
    each value is a list of per-station results (DataFrames, dicts, or
    xarray Datasets).  Returns a single merged result.
    """
    # Prune any modalities that returned no data
    for k in list(accumulated_data.keys()):
        if not accumulated_data[k]:
            del accumulated_data[k]

    if not accumulated_data:
        if as_xarray_dataset:
            return xarray.Dataset()
        return {}

    # Determine return type from the first available data item
    first_key = list(accumulated_data.keys())[0]
    first_item = accumulated_data[first_key][0]

    return_as_df = isinstance(first_item, pd.DataFrame)
    use_opendap = isinstance(first_item, xarray.Dataset)

    # Flatten all data into a single list if df or xarray
    if return_as_df or use_opendap:
        data_list = []
        for mode, station_data in accumulated_data.items():
            data_list.extend(station_data)

        if not data_list:
            return pd.DataFrame() if return_as_df else xarray.Dataset()

    else:
        # For dict response, return data grouped by modality.
        # Coalescence does not apply to this structure.
        return accumulated_data

    if return_as_df:
        df = pd.concat(data_list, axis=0)
        if df.empty:
            return df

        df.reset_index(inplace=True, drop=False)

        # Group by the intended index to merge rows for the same timestamp
        index_cols = ['timestamp', 'station_id']

        present_index_cols = [
            col for col in index_cols if col in df.columns
        ]
        if not present_index_cols:
            return df

        # Aggregate all other columns by taking the first non-null value
        agg_cols = [
            col for col in df.columns if col not in present_index_cols
        ]

        # Only aggregate if there are columns to aggregate
        if agg_cols:
            agg_funcs = {col: 'first' for col in agg_cols}
            df = df.groupby(present_index_cols,
                            as_index=False).agg(agg_funcs)
        else:
            df = df.drop_duplicates(subset=present_index_cols)

        df.set_index(present_index_cols, inplace=True)
        # Normalize null representation: concat/groupby may
        # introduce None for object-dtype columns.
        return df.where(df.notna())

    elif use_opendap:
        # xarray's merge function handles this type of coalescence.
        return merge_datasets(data_list)
    if as_xarray_dataset:  # pragma: no cover
        return xarray.Dataset()
    return {}  # pragma: no cover
