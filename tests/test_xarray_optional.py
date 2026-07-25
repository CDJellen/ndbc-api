import sys
import unittest
from unittest.mock import patch

import pytest


class TestXarrayOptional(unittest.TestCase):

    def setUp(self):
        # We need to clear the imported modules from sys.modules to simulate a fresh import
        # where xarray is not available.
        self.modules_to_unload = [
            'ndbc_api',
            'ndbc_api.ndbc_api',
            'ndbc_api.async_ndbc_api',
            'ndbc_api.utilities.data_helpers',
            'ndbc_api.utilities.opendap.dataset',
            'ndbc_api.api.handlers.opendap.data',
            'ndbc_api.api.parsers.opendap._base',
        ]
        self.original_modules = {}
        for mod in list(sys.modules.keys()):
            if mod.startswith('ndbc_api'):
                self.original_modules[mod] = sys.modules.pop(mod)

    def tearDown(self):
        # Restore original modules
        for mod in list(sys.modules.keys()):
            if mod.startswith('ndbc_api'):
                sys.modules.pop(mod)
        sys.modules.update(self.original_modules)

    @patch.dict('sys.modules', {'xarray': None})
    def test_import_without_xarray(self):
        """Test that NdbcApi can be imported without xarray installed."""
        try:
            from ndbc_api import NdbcApi
            from ndbc_api import AsyncNdbcApi
        except ImportError as e:
            self.fail(f"ImportError raised unexpectedly: {e}")

    @patch.dict('sys.modules', {'xarray': None})
    def test_get_data_without_xarray(self):
        """Test that standard get_data works without xarray."""
        from ndbc_api import NdbcApi
        api = NdbcApi()
        
        # Mock the handler to avoid actual network requests
        with patch.object(api._handler, 'handle_request') as mock_handle:
            # We just want to ensure it doesn't crash before making the request
            # or during the setup phase.
            try:
                # This will likely fail due to the mock not returning a valid response,
                # but it shouldn't fail due to an ImportError.
                api.get_data(station_id='tplm2', mode='stdmet', as_df=False)
            except Exception as e:
                # We expect a ResponseException or similar because of the mock,
                # but NOT an ImportError related to xarray.
                self.assertNotIsInstance(e, ImportError)

    @patch.dict('sys.modules', {'xarray': None})
    def test_get_data_as_xarray_dataset_raises_error(self):
        """Test that requesting as_xarray_dataset raises an ImportError."""
        from ndbc_api import NdbcApi
        api = NdbcApi()
        
        with self.assertRaises(ImportError) as context:
            api.get_data(station_id='tplm2', mode='stdmet', as_xarray_dataset=True)
            
        self.assertIn("xarray is required for OpenDAP support", str(context.exception))

    @patch.dict('sys.modules', {'xarray': None})
    def test_save_xarray_dataset_raises_error(self):
        """Test that save_xarray_dataset raises an ImportError."""
        from ndbc_api import NdbcApi
        
        with self.assertRaises(ImportError) as context:
            NdbcApi.save_xarray_dataset(None, 'test.nc')
            
        self.assertIn("xarray is required for OpenDAP support", str(context.exception))

if __name__ == '__main__':
    unittest.main()
