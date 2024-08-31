import os
import tempfile
from typing import List

import netCDF4 as nc
import numpy as np

from ndbc_api.exceptions import ParserException


class BaseParser:

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude']

    @classmethod
    def nc_from_responses(cls,
                          responses: List[dict],
                          use_timestamp: bool = False,
                          ) -> 'nc.Dataset':
        """Build the netCDF dataset from the responses.
        
        Args: 
            responses (List[dict]): All responses from the THREDDS
                server regardless of content or HTTP code.
        
        Returns:
            nc.Dataset: The netCDF dataset.
        """
        datasets = []
        for r in responses:
            if r.get("status") != 200:
                continue
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.nc', dir=os.getcwd()) as temp_file:
                    temp_file.write(r["body"])
                    temp_file.flush()
                    temp_file.close()

                    ds = nc.Dataset(temp_file.name, 'r', format='NETCDF4')
                    datasets.append(ds)

                    os.remove(temp_file.name)
                datasets.append(ds)
            except Exception as e:
                raise ParserException from e

        return cls._join_netcdf4(datasets)

    @classmethod
    def _join_netcdf4(
            cls,
            datasets: List['nc.Dataset'],
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
        for dim_name in [cls.TEMPORAL_DIM] + cls.SPATIAL_DIMS:
            all_values = np.concatenate([ds.variables[dim_name][:] for ds in datasets])
            unique_values, indices = np.unique(all_values, return_inverse=True)
            unique_dim_values[dim_name] = unique_values

        with tempfile.NamedTemporaryFile(delete=True, suffix='.nc', dir=os.getcwd()) as temp_file:
            output_ds = nc.Dataset(temp_file.name, 'w', format='NETCDF4')

        # create time dimension
        output_ds.createDimension(cls.TEMPORAL_DIM, len(unique_dim_values[cls.TEMPORAL_DIM]))
        time_var = output_ds.createVariable(cls.TEMPORAL_DIM, 'f8', (cls.TEMPORAL_DIM,))
        time_var[:] = unique_dim_values[cls.TEMPORAL_DIM]
        time_var.units = datasets[0].variables[cls.TEMPORAL_DIM].units 

        # create spatial dimensions
        for dim_name in cls.SPATIAL_DIMS:
            output_ds.createDimension(dim_name, len(unique_dim_values[dim_name]))
            dim_var = output_ds.createVariable(dim_name, 'f8', (dim_name,))
            dim_var[:] = unique_dim_values[dim_name]
            if hasattr(datasets[0].variables[dim_name], 'units'):
                dim_var.units = datasets[0].variables[dim_name].units
        
        all_variables = set()
        for ds in datasets:
            all_variables.update(ds.variables.keys())
        
        # remove the dimensions from the list of variables
        all_variables -= {cls.TEMPORAL_DIM, *cls.SPATIAL_DIMS}

        for var_name in all_variables:
            datasets_with_var = [ds for ds in datasets if var_name in ds.variables]
            if not datasets_with_var:
                continue

            var1 = datasets_with_var[0].variables[var_name]
            fill_value = getattr(var1, '_FillValue', np.nan) 

            out_var = output_ds.createVariable(var_name, var1.datatype, var1.dimensions, fill_value=fill_value)
            out_var.setncatts(var1.__dict__)

            # create an empty array to store the merged data
            combined_data = np.full((len(unique_dim_values[cls.TEMPORAL_DIM]),) + var1.shape[1:], fill_value, dtype=var1.datatype)

            # fill in data from each dataset based on their time indices
            for ds in datasets_with_var:
                var = ds.variables[var_name]
                time_values = ds.variables[cls.TEMPORAL_DIM][:]
                indices = np.where(np.in1d(unique_dim_values[cls.TEMPORAL_DIM], time_values))[0]
                combined_data[indices, ...] = var[:]

            out_var[:] = combined_data

        return output_ds
