from datetime import datetime
from typing import List, Union

import xarray
import numpy as np


def concat_datasets(
    datasets: List[xarray.Dataset],
    temporal_dim_name: str = 'time',
) -> xarray.Dataset:
    """Joins multiple xarray datasets using their shared dimensions.

    Handles cases where datasets might not have the same variables, 
    but requires that all datasets share the same dimensions. For
    data stored on the THREDDS server, all datasets are expected to
    have `time`, `latitude`, and `longitude` dimensions.

    Args:
        datasets (List[xarray.Dataset]): A list of xarray datasets
            to join.
        dimension_names (List[str]): A list of dimension names to join
            the datasets on. Defaults to `['time', 'latitude', 'longitude']`.
    
    Returns:
        A xarray.Dataset object containing the joined data.
    """
    result = xarray.concat(datasets, dim=temporal_dim_name)
    return result


def merge_datasets(datasets: List[xarray.Dataset],) -> xarray.Dataset:
    """Merges multiple xarray datasets using their shared dimensions.

    Handles cases where datasets might not have the same variables,
    but requires that all datasets share the same dimensions. For
    data stored on the THREDDS server, all datasets are expected to
    have `time`, `latitude`, and `longitude` dimensions.

    Args:
        datasets (List[xarray.Dataset]): A list of xarray datasets
            to join.

    Returns:
        A xarray.Dataset object containing the merged data.
    """
    result = xarray.merge(datasets, compat='override')
    return result


def filter_dataset_by_time_range(
    dataset: xarray.Dataset,
    start_time: datetime,
    end_time: datetime,
    temporal_dim_name: str = 'time',
) -> xarray.Dataset:
    """
    Filters a netCDF4 Dataset to keep only data within a specified time range.

    Args:
        dataset: The netCDF4 Dataset object.
        start_time: The start of the time range (inclusive) as an ISO 8601 string (e.g., '2023-01-01T00:00:00Z').
        end_time: The end of the time range (inclusive) as an ISO 8601 string.

    Returns:
        The modified netCDF4 Dataset object with data outside the time range removed.
    """
    filtered_ds = dataset.sel({temporal_dim_name: slice(start_time, end_time)})
    return filtered_ds


def filter_dataset_by_variable(
    dataset: xarray.Dataset,
    cols: Union[List[str], None] = None,
) -> xarray.Dataset:
    """
    Filters a netCDF4 Dataset to keep only data with variables whose names are in cols.

    Args:
        dataset: The netCDF4 Dataset object.
        cols: A list of variable names to keep.

    Returns:
        The modified netCDF4 Dataset object with data with variables not in cols removed.
    """
    if cols is None:
        return dataset
    return dataset[cols]
