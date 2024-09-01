import os
import tempfile
from typing import List

import netCDF4 as nc

from ndbc_api.exceptions import ParserException
from ndbc_api.utilities.opendap.dataset import join_netcdf4


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
            if isinstance(r, dict):
                if 'status' in r and r.get("status") != 200:
                    continue
                content = r['body']
            else:
                content = r
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.nc', dir=os.getcwd()) as temp_file:
                    temp_file.write(content)
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
        return join_netcdf4(
            datasets,
            cls.TEMPORAL_DIM,
        )
