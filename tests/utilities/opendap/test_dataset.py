from datetime import datetime

import pytest
import xarray

from ndbc_api.utilities.opendap.dataset import (concat_datasets,
                                                merge_datasets,
                                                filter_dataset_by_variable,
                                                filter_dataset_by_time_range)
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR

STDMET_TEST_FP = PARSED_TESTS_DIR.joinpath('stdmet.nc')
CWIND_TEST_FP = PARSED_TESTS_DIR.joinpath('cwind.nc')


@pytest.fixture
def parsed_stdmet():
    ds = xarray.open_dataset(STDMET_TEST_FP)
    yield ds


@pytest.fixture
def parsed_cwind():
    ds = xarray.open_dataset(CWIND_TEST_FP)
    yield ds


@pytest.mark.slow
def test_merge_datasets(parsed_stdmet, parsed_cwind):
    got = merge_datasets([parsed_stdmet, parsed_cwind])
    assert set(parsed_stdmet.variables).issubset(set(
        got.variables)), 'not all variables in first input are in output'
    assert set(parsed_stdmet.variables).issubset(set(
        got.variables)), 'not all variables in the second input are in output'
