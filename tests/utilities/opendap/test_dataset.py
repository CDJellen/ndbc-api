from datetime import datetime

import pytest
import netCDF4 as nc

from ndbc_api.utilities.opendap.dataset import (
    join_netcdf4,
    filter_netcdf4_by_variable,
    filter_netcdf4_by_time_range
)
from tests.api.parsers.opendap._base import PARSED_TESTS_DIR

STDMET_TEST_FP = PARSED_TESTS_DIR.joinpath('stdmet.nc')
CWIND_TEST_FP = PARSED_TESTS_DIR.joinpath('cwind.nc')


@pytest.fixture
def parsed_stdmet():
    ds = nc.Dataset(STDMET_TEST_FP, 'r')
    yield ds


@pytest.fixture
def parsed_cwind():
    ds = nc.Dataset(CWIND_TEST_FP, 'r')
    yield ds


@pytest.mark.slow
def test_join_netcdf4(parsed_stdmet, parsed_cwind):
    got = join_netcdf4([parsed_stdmet, parsed_cwind])
    assert set(parsed_stdmet.variables).issubset(set(got.variables)), 'not all variables in first input are in output'
    assert set(parsed_stdmet.variables).issubset(set(got.variables)), 'not all variables in the second input are in output'

@pytest.mark.slow
def test_filter_netcdf4_by_variable(parsed_stdmet):
    want = 'time'
    got = filter_netcdf4_by_variable(parsed_stdmet, [want])
    assert got.variables[want] is not None

@pytest.mark.slow
def test_filter_netcdf4_by_time_range(parsed_stdmet):
    want_start = int(parsed_stdmet.variables['time'][50].data)
    want_end = int(parsed_stdmet.variables['time'][100].data)
    want_start_dt = datetime.utcfromtimestamp(want_start)
    want_end_dt = datetime.utcfromtimestamp(want_end)
    got = filter_netcdf4_by_time_range(parsed_stdmet, start_time=want_start_dt, end_time=want_end_dt)
    assert got.variables['time'][0].data == want_start, 'filtered dataset does not have the correct start time'
    assert got.variables['time'][-1].data == want_end, 'filtered dataset does not have the correct end time'
