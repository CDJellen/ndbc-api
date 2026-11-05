import io
import os
import tempfile
from typing import List, Optional

try:
    import xarray
except ImportError:
    xarray = None

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
    ) -> 'xarray.Dataset':
        """Build the netCDF dataset from the responses.
        
        Args: 
            responses (List[dict]): All responses from the THREDDS
                server regardless of content or HTTP code.
        
        Returns:
            xarray.Dataset: The netCDF dataset.
        """
        if xarray is None:
            raise ImportError(
                'xarray is required for OpenDAP support. If you uninstalled it '
                'to create a lightweight environment, you must reinstall it to use this feature.'
            )
        datasets = []
        for r in responses:
            if isinstance(r, dict):
                if 'status' in r and r.get('status') != 200:
                    continue
                content = r['body']
            else:
                content = r
            try:
                if isinstance(content, (bytes, bytearray)) and content.startswith(b'\x89HDF\r'):
                    try:
                        with xarray.open_dataset(io.BytesIO(content), engine='h5netcdf') as ds:
                            xrds = ds.load()
                    except Exception:
                        tmp_file_path = None
                        try:
                            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                                tmp_file.write(content)
                                tmp_file_path = tmp_file.name
                            with xarray.open_dataset(tmp_file_path, engine='h5netcdf') as ds:
                                xrds = ds.load()
                        finally:
                            if tmp_file_path and os.path.exists(tmp_file_path):
                                os.remove(tmp_file_path)
                else:
                    xrds = xarray.open_dataset(content)
                datasets.append(xrds)
            except Exception as e:
                raise ParserException from e

        return cls._merge_datasets(datasets)

    """ PRIVATE """

    @classmethod
    def _merge_datasets(
        cls,
        datasets: List['xarray.Dataset'],
        temporal_dim_name: Optional[str] = None,
    ) -> 'xarray.Dataset':
        """Joins multiple xarray datasets using their shared dimensions."""
        return concat_datasets(
            datasets,
            temporal_dim_name if temporal_dim_name else cls.TEMPORAL_DIM,
        )
