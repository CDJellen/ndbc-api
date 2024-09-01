import tempfile
import os
from datetime import datetime
from typing import List, Union

import netCDF4 as nc
import numpy as np


def join_netcdf4(
    datasets: List['nc.Dataset'],
    temporal_dim_name: str = 'time',
) -> 'nc.Dataset':
    """Joins multiple netCDF4 datasets using their shared dimensions.

    Handles cases where datasets might not have the same variables, 
    but requires that all datasets share the same dimensions. For
    data stored on the THREDDS server, all datasets are expected to
    have `time`, `latitude`, and `longitude` dimensions.

    Args:
        datasets (List[netCDF4.Dataset]): A list of netCDF4 datasets
            to join.
        dimension_names (List[str]): A list of dimension names to join
            the datasets on. Defaults to `['time', 'latitude', 'longitude']`.
    
    Returns:
        A netCDF4.Dataset object containing the joined data.
    """
    unique_dim_values = {}
    for dim_name in datasets[0].dimensions.keys():
        all_values = np.concatenate([ds.variables[dim_name][:] for ds in datasets])
        unique_values, indices = np.unique(all_values, return_inverse=True)
        unique_dim_values[dim_name] = unique_values

    with tempfile.NamedTemporaryFile(delete=True, suffix='.nc', dir=os.getcwd()) as temp_file:
        output_ds = nc.Dataset(temp_file.name, 'w', format='NETCDF4')
    
    # create time dimension
    output_ds.createDimension(temporal_dim_name, len(unique_dim_values[temporal_dim_name]))
    temporal_var = datasets[0].variables[temporal_dim_name]
    fill_value = getattr(temporal_var, '_FillValue', 'f8')
    time_var = output_ds.createVariable(temporal_dim_name, fill_value, (temporal_dim_name,))
    time_var[:] = unique_dim_values[temporal_dim_name]
    time_var.units = datasets[0].variables[temporal_dim_name].units 

    # create other spatial dimensions
    for dim_name in unique_dim_values:
        if dim_name == temporal_dim_name:
            continue
        output_ds.createDimension(dim_name, len(unique_dim_values[dim_name]))
        spatial_var = datasets[0].variables[dim_name]
        fill_value = getattr(spatial_var, '_FillValue', 'f8')
        dim_var = output_ds.createVariable(dim_name, fill_value, (dim_name,))
        dim_var[:] = unique_dim_values[dim_name]
        if hasattr(datasets[0].variables[dim_name], 'units'):
            dim_var.units = datasets[0].variables[dim_name].units
    
    all_variables = set()
    for ds in datasets:
        all_variables.update(ds.variables.keys())

    for var_name in all_variables:
        if var_name in unique_dim_values:
            continue
        datasets_with_var = [ds for ds in datasets if var_name in ds.variables]
        if not datasets_with_var:
            continue

        var1 = datasets_with_var[0].variables[var_name]
        fill_value = getattr(var1, '_FillValue', None)
        if not fill_value:
            continue

        out_var = output_ds.createVariable(var_name, var1.datatype, var1.dimensions, fill_value=fill_value)
        out_var.setncatts(var1.__dict__)

        # create an empty array to store the merged data
        combined_data = np.full((len(unique_dim_values[temporal_dim_name]),) + var1.shape[1:], fill_value, dtype=var1.datatype)

        # fill in data from each dataset based on their time indices
        for ds in datasets_with_var:
            var = ds.variables[var_name]
            time_values = ds.variables[temporal_dim_name][:]
            indices = np.where(np.in1d(unique_dim_values[temporal_dim_name], time_values))[0]
            combined_data[indices, ...] = var[:]

        out_var[:] = combined_data

    return output_ds

def filter_netcdf4_by_time_range(
    dataset: nc.Dataset,
    start_time: datetime,
    end_time: datetime,
    temporal_dim_name: str = 'time',
) -> nc.Dataset:
    """
    Filters a netCDF4 Dataset to keep only data within a specified time range.

    Args:
        dataset: The netCDF4 Dataset object.
        start_time: The start of the time range (inclusive) as an ISO 8601 string (e.g., '2023-01-01T00:00:00Z').
        end_time: The end of the time range (inclusive) as an ISO 8601 string.

    Returns:
        The modified netCDF4 Dataset object with data outside the time range removed.
    """
    with tempfile.NamedTemporaryFile(delete=True, suffix='.nc', dir=os.getcwd()) as temp_file:
        output_ds = nc.Dataset(temp_file.name, 'w', format='NETCDF4')

    time_var = dataset.variables[temporal_dim_name]
    time_units = time_var.units

    start_time_num = nc.date2num(start_time, time_units)
    end_time_num = nc.date2num(end_time, time_units)

    time_values = time_var[:].squeeze()
    time_indices = np.where((time_values >= start_time_num) & (time_values <= end_time_num))[0]

    output_ds.createDimension(temporal_dim_name, len(time_indices))
    new_time_var = output_ds.createVariable(temporal_dim_name, time_var.datatype, (temporal_dim_name,))
    new_time_var.units = time_var.units
    new_time_var[:] = time_values[time_indices]

    # copy the spatial dimensions
    for dim_name in dataset.dimensions.keys():
        if dim_name == temporal_dim_name:
            continue
        dim_var = dataset.variables[dim_name]
        output_ds.createDimension(dim_name, len(dim_var))
        out_var = output_ds.createVariable(dim_name, dim_var.datatype, (dim_name,))
        if hasattr(dim_var, 'units'):
          out_var.units = dim_var.units
        out_var[:] = dim_var[:]

    for var_name, var in dataset.variables.items():
        if var_name in dataset.dimensions:
            continue
        if temporal_dim_name in var.dimensions:
            var_data = var[:]
            if var_data.ndim > 1:
                filtered_data = var_data[time_indices, ...]
                new_shape = (len(time_indices),) + var_data.shape[1:]
            else:
                filtered_data = var_data[time_indices]
                new_shape = (len(time_indices),)
            new_var = output_ds.createVariable(var_name, var.datatype, var.dimensions, fill_value=getattr(var, '_FillValue', None))
            new_var.setncatts(var.__dict__)
            new_var[:] = filtered_data.reshape(new_shape)

    return output_ds

def filter_netcdf4_by_variable(
    dataset: nc.Dataset,
    cols: Union[List[str], None] = None,
) -> nc.Dataset:
    """
    Filters a netCDF4 Dataset to keep only data with variables whose names are in cols.

    Args:
        dataset: The netCDF4 Dataset object.
        cols: A list of variable names to keep.

    Returns:
        The modified netCDF4 Dataset object with data with variables not in cols removed.
    """
    with tempfile.NamedTemporaryFile(delete=True, suffix='.nc', dir=os.getcwd()) as temp_file:
        output_ds = nc.Dataset(temp_file.name, 'w', format='NETCDF4')
    
    # copy dimensions
    for dim_name in dataset.dimensions.keys():
        dim_var = dataset.variables[dim_name]
        output_ds.createDimension(dim_name, len(dim_var))
        out_var = output_ds.createVariable(dim_name, dim_var.datatype, (dim_name,))
        if hasattr(dim_var, 'units'):
          out_var.units = dim_var.units
        out_var[:] = dim_var[:]
    
    # copy variables
    if not cols:
        cols = dataset.variables.keys()
    for var_name in cols:
        if var_name in dataset.dimensions.keys():
            continue
        var = dataset.variables[var_name]
        out_var = output_ds.createVariable(var_name, var.datatype, var.dimensions)
        out_var.setncatts(var.__dict__)
        out_var[:] = var[:]
    
    return output_ds
