import os
import tempfile
from typing import List, Optional

import xarray
import xarray

from ndbc_api.exceptions import ParserException
from ndbc_api.utilities.opendap.dataset import concat_datasets


class BaseParser:

    TEMPORAL_DIM = 'time'
    SPATIAL_DIMS = ['latitude', 'longitude']

    @classmethod
    def nc_from_responses(
        cls,
        responses: List[dict],
        use_timestamp: bool = False,
    ) -> xarray.Dataset:
        """Build the netCDF dataset from the responses.
        
        Args: 
            responses (List[dict]): All responses from the THREDDS
                server regardless of content or HTTP code.
        
        Returns:
            xarray.open_dataset: The netCDF dataset.
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
                # if the content bytes begin with b'\x1f\x8b\x08' we need to write to a temporary file
                if content.startswith(b'\x89HDF\r'):
                    # This is a gzip-compressed HDF5 file, we need to write it to a temporary file
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        tmp_file.write(content)
                        tmp_file_path = tmp_file.name
                    xrds = xarray.open_dataset(tmp_file_path, engine='h5netcdf')
                    os.remove(tmp_file_path)
                else:
                    xrds = xarray.open_dataset(content, engine='h5netcdf')
                datasets.append(xrds)
            except Exception as e:
                raise ParserException from e

        return cls._merge_datasets(datasets)

    @classmethod
    def _merge_datasets(
        cls,
        datasets: List[xarray.Dataset],
        temporal_dim_name: Optional[str] = None,
    ) -> xarray.Dataset:
        """Joins multiple xarray datasets using their shared dimensions.

        Handles cases where datasets might not have the same variables, 
        but requires that all datasets share the same dimensions. For
        data stored on the THREDDS server, all datasets are expected to
        have `time`, `latitude`, and `longitude` dimensions.

        Args:
            temporal_dim_name (List[xarray.Dataset]): A list of netCDF4 datasets
                to join.
            dimension_names (List[str]): A list of dimension names to join
                the datasets on. Defaults to `['time', 'latitude', 'longitude']`.
        
        Returns:
            A netCDF4.Dataset object containing the joined data.
        """
        return concat_datasets(
            datasets,
            temporal_dim_name if temporal_dim_name else cls.TEMPORAL_DIM,
        )
