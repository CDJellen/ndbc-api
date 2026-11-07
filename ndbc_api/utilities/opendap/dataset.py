from datetime import datetime
from typing import List, Union

from ndbc_api.utilities.mode_metadata import COLUMN_UNITS

try:
    import xarray
except ImportError:
    xarray = None

_XARRAY_IMPORT_ERROR = (
    'xarray is required for OpenDAP support. If you uninstalled it '
    'to create a lightweight environment, you must reinstall it to use this feature.'
)


def _attach_variable_units(dataset: 'xarray.Dataset') -> 'xarray.Dataset':
    """Populate `units` in `var.attrs` for recognized NDBC variables lacking them."""
    for var_name in dataset.data_vars:
        if var_name in COLUMN_UNITS and 'units' not in dataset[var_name].attrs:
            dataset[var_name].attrs['units'] = COLUMN_UNITS[var_name]
    return dataset


def concat_datasets(
    datasets: List['xarray.Dataset'],
    temporal_dim_name: str = 'time',
) -> 'xarray.Dataset':
    """Joins multiple xarray datasets using their shared dimensions.

    Handles cases where datasets might not have the same variables, 
    but requires that all datasets share the same dimensions. For
    data stored on the THREDDS server, all datasets are expected to
    have `time`, `latitude`, and `longitude` dimensions.

    Args:
        datasets (List[xarray.Dataset]): A list of xarray datasets
            to join.
        temporal_dim_name (str): Dimension name to concatenate along.
            Defaults to `'time'`.
    
    Returns:
        A xarray.Dataset object containing the joined data.
    """
    if xarray is None:
        raise ImportError(_XARRAY_IMPORT_ERROR)
    if not datasets:
        return xarray.Dataset()
    result = xarray.concat(datasets, dim=temporal_dim_name)
    if temporal_dim_name in result.coords or temporal_dim_name in result.dims:
        try:
            if temporal_dim_name in result.indexes:
                idx = result.indexes[temporal_dim_name]
                if getattr(idx, 'has_duplicates', False):
                    result = result.isel(
                        {temporal_dim_name: ~idx.duplicated(keep='first')}
                    )
            result = result.sortby(temporal_dim_name)
        except Exception:
            pass
    return _attach_variable_units(result)


def merge_datasets(datasets: List['xarray.Dataset'],) -> 'xarray.Dataset':
    """Merges multiple xarray datasets using their shared dimensions.

    Handles cases where datasets might not have the same variables,
    but requires that all datasets share the same dimensions. When datasets
    from multiple stations are tagged with a `station_id` coordinate or
    attribute, variables are first merged per station and then concatenated
    along the `station_id` dimension so no station's data is overwritten.

    Args:
        datasets (List[xarray.Dataset]): A list of xarray datasets
            to join.

    Returns:
        A xarray.Dataset object containing the merged data.
    """
    if xarray is None:
        raise ImportError(_XARRAY_IMPORT_ERROR)
    if not datasets:
        return xarray.Dataset()

    # Check whether datasets carry distinct station_id tags
    station_groups = {}
    untagged = []
    for ds in datasets:
        if not ds.data_vars and not ds.coords:
            continue
        sid = None
        if 'station_id' in ds.coords:
            val = ds.coords['station_id'].values
            sid = str(val.item() if hasattr(val, 'item') and val.ndim == 0 else val[0])
        elif 'station_id' in ds.attrs:
            sid = str(ds.attrs['station_id'])
        if sid is not None:
            station_groups.setdefault(sid, []).append(ds)
        else:
            untagged.append(ds)

    if len(station_groups) >= 1:
        per_station = []
        for sid, stn_ds_list in station_groups.items():
            merged_stn = xarray.merge(stn_ds_list, compat='override')
            if 'station_id' not in merged_stn.dims:
                if 'station_id' in merged_stn.coords:
                    merged_stn = merged_stn.expand_dims('station_id')
                else:
                    merged_stn = merged_stn.expand_dims({'station_id': [sid]})
            per_station.append(merged_stn)
        result = xarray.concat(per_station, dim='station_id')
        if untagged:
            result = xarray.merge([result, *untagged], compat='override')
        return _attach_variable_units(result)

    result = xarray.merge(datasets, compat='override')
    return _attach_variable_units(result)


def filter_dataset_by_time_range(
    dataset: 'xarray.Dataset',
    start_time: datetime,
    end_time: datetime,
    temporal_dim_name: str = 'time',
) -> 'xarray.Dataset':
    """
    Filters a netCDF4 Dataset to keep only data within a specified time range.

    Args:
        dataset: The netCDF4 Dataset object.
        start_time: The start of the time range (inclusive) as a datetime or ISO 8601 string.
        end_time: The end of the time range (inclusive) as a datetime or ISO 8601 string.

    Returns:
        The modified netCDF4 Dataset object with data outside the time range removed.
    """
    if xarray is None:
        raise ImportError(_XARRAY_IMPORT_ERROR)
    if temporal_dim_name not in dataset.coords and temporal_dim_name not in dataset.dims:
        return dataset
    filtered_ds = dataset.sel({temporal_dim_name: slice(start_time, end_time)})
    return filtered_ds


def filter_dataset_by_variable(
    dataset: 'xarray.Dataset',
    cols: Union[List[str], None] = None,
) -> 'xarray.Dataset':
    """
    Filters a netCDF4 Dataset to keep only data with variables whose names are in cols.

    Args:
        dataset: The netCDF4 Dataset object.
        cols: A list of variable names to keep.

    Returns:
        The modified netCDF4 Dataset object with data with variables not in cols removed.
    """
    if xarray is None:
        raise ImportError(_XARRAY_IMPORT_ERROR)
    if cols is None:
        return dataset
    return dataset[cols]
