"""Tests for first-principles scientific integrity, concurrency, and API extensions."""
from datetime import date, datetime, timedelta, timezone
import logging
from pathlib import Path

import pandas as pd
import pytest
import xarray

from ndbc_api import (
    COLUMN_UNITS,
    MODE_METADATA,
    AsyncNdbcApi,
    HandlerException,
    NdbcApi,
    NdbcException,
    ParserException,
    RequestException,
    ResponseException,
    TimestampException,
    describe_mode,
)
from ndbc_api.api.dispatch import (
    HTTP_DISPATCH,
    OPENDAP_DISPATCH,
    build_mode_requests,
)
from ndbc_api.api.handlers.http.data import DataHandler
from ndbc_api.api.handlers.http.stations import StationsHandler
from ndbc_api.api.handlers.opendap.data import OpenDapDataHandler
from ndbc_api.api.parsers.http._base import BaseParser
from ndbc_api.api.parsers.http.active_stations import ActiveStationsParser
from ndbc_api.api.parsers.http.adcp import AdcpParser
from ndbc_api.api.parsers.http.cwind import CwindParser
from ndbc_api.api.parsers.http.historical_stations import HistoricalStationsParser
from ndbc_api.api.parsers.http.spec import SpecParser
from ndbc_api.api.parsers.http.station_historical import HistoricalParser
from ndbc_api.api.parsers.http.station_metadata import MetadataParser
from ndbc_api.api.parsers.http.stdmet import StdmetParser
from ndbc_api.api.parsers.http.supl import SuplParser
from ndbc_api.api.parsers.http.swdir import SwdirParser
from ndbc_api.api.parsers.http.swdir2 import Swdir2Parser
from ndbc_api.api.requests.http.stdmet import StdmetRequest
from ndbc_api.utilities.data_helpers import (
    enforce_timerange,
    extract_realtime_modes,
    filter_station_deployments,
    handle_accumulate_data,
    handle_data,
    handle_timestamp,
    summarize_historical_modes,
)
from ndbc_api.utilities.log_formatter import LogFormatter
from ndbc_api.utilities.opendap.dataset import (
    concat_datasets,
    filter_dataset_by_time_range,
    merge_datasets,
)
from ndbc_api.utilities.req_cache import RequestCache


# ===========================================================================
# Tier 1: Scientific & Data Integrity
# ===========================================================================


class TestSentinelDisambiguation:
    """Verify 99 deg (East) and 999.0 hPa (low pressure) are never erased."""

    def test_stdmet_preserves_99_deg_and_999_hpa(self):
        body = (
            '#YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE\n'
            '#yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft\n'
            '2022 09 28 12 00  99  25.4 32.1  6.50  12.0   9.5  99  999.0  26.5  27.8  24.1 99.0 -3.2 99.00\n'
            '2022 09 28 13 00 999 99.0 99.0 99.00 99.00 99.00 999 9999.0 999.0 999.0 999.0   MM   MM    MM\n'
        )
        got = StdmetParser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        assert len(got) == 2

        # Row 0: valid storm observations (WDIR=99 deg, MWD=99 deg, PRES=999.0 hPa)
        assert got[0]['WDIR'] == 99
        assert got[0]['MWD'] == 99
        assert got[0]['PRES'] == 999.0
        assert got[0]['VIS'] is None
        assert got[0]['TIDE'] is None

        # Row 1: genuine missing sentinels (WDIR=999, MWD=999, PRES=9999.0, MM)
        assert got[1]['WDIR'] is None
        assert got[1]['MWD'] is None
        assert got[1]['PRES'] is None
        assert got[1]['ATMP'] is None
        assert got[1]['PTDY'] is None

    def test_cwind_preserves_99_deg(self):
        body = (
            '#YY  MM DD hh mm WDIR WSPD GDR GST GTIME\n'
            '#yr  mo dy hr mn degT m/s degT m/s hhmm\n'
            '2022 01 01 00 00  99 12.3  99 15.1 0005\n'
            '2022 01 01 00 10 999 99.0 999 99.0 9999\n'
        )
        got = CwindParser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        assert got[0]['WDIR'] == 99
        assert got[0]['GDR'] == 99
        assert got[1]['WDIR'] is None
        assert got[1]['GDR'] is None
        assert got[1]['WSPD'] is None

    def test_supl_preserves_999_hpa_and_99_deg(self):
        body = (
            '#YY  MM DD hh mm PRES PTIME WSPD WDIR WTIME\n'
            '#yr  mo dy hr mn  hPa  hhmm  m/s degT  hhmm\n'
            '2022 01 01 00 00 999.0 0001 18.5   99  0002\n'
            '2022 01 01 01 00 9999.0 9999 99.0 999  9999\n'
        )
        got = SuplParser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        assert got[0]['PRES'] == 999.0
        assert got[0]['WDIR'] == 99
        assert got[1]['PRES'] is None
        assert got[1]['WDIR'] is None
        assert got[1]['WSPD'] is None

    def test_spec_handles_mm_and_99_deg_mwd(self):
        body = (
            '#YY  MM DD hh mm WVHT  SwH  SwP  WWH  WWP SwD WWD  STEEPNESS  APD MWD\n'
            '#yr  mo dy hr mn    m    m  sec    m  sec  -  degT     -      sec degT\n'
            '2022 01 01 00 00  1.5  1.2  8.0  0.4  4.5  SE  MM      AVERAGE 5.5  99\n'
            '2022 01 01 01 00 99.0 99.0 99.0   MM   MM  MM  MM          N/A 99.0 999\n'
        )
        df = SpecParser.df_from_responses([{'status': 200, 'body': body}], use_timestamp=True)
        assert df.loc[datetime(2022, 1, 1, 0, 0), 'MWD'] == 99
        assert df.loc[datetime(2022, 1, 1, 0, 0), 'WWH'] == pytest.approx(0.4)
        assert pd.isna(df.loc[datetime(2022, 1, 1, 1, 0), 'WWH'])
        assert pd.isna(df.loc[datetime(2022, 1, 1, 1, 0), 'MWD'])
        assert pd.api.types.is_numeric_dtype(df['SwH'])
        assert pd.api.types.is_numeric_dtype(df['WWH'])

    def test_swdir_and_adcp_preserve_integer_99_deg(self):
        body = (
            '#YY  MM DD hh mm 0.033 0.040\n'
            '2022 01 01 00 00    99   999\n'
        )
        swdir_rows = SwdirParser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        swdir2_rows = Swdir2Parser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        adcp_rows = AdcpParser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        assert swdir_rows[0]['0.033'] == 99
        assert swdir_rows[0]['0.040'] is None
        assert swdir2_rows[0]['0.033'] == 99
        assert adcp_rows[0]['0.033'] == 99


class TestMultiYearMonthlyRequests:
    """Verify multi-year queries include Jan..start_time.month of the final year."""

    def test_multi_year_includes_early_months_of_current_year(self, monkeypatch):
        monkeypatch.setenv('MOCKDATE', '2022-08-13')
        reqs = StdmetRequest.build_request(
            station_id='tplm2',
            start_time=datetime(2020, 6, 1),
            end_time=datetime(2022, 8, 10),
        )
        # Months 1..6 of 2022 must all be requested because start_time.year (2020) < 2022
        for month_code in ('1', '2', '3', '4', '5', '6'):
            assert any(f'tplm2{month_code}2022.txt.gz' in r for r in reqs), (
                f'Missing month {month_code} of 2022 in {reqs}'
            )


class TestMultiFileSchemaUnion:
    """Verify columns appearing only in later responses (e.g. realtime PTDY) are preserved."""

    def test_historical_plus_realtime_column_union(self):
        hist_body = (
            "#YY  MM DD hh mm WDIR WSPD\n"
            "#yr  mo dy hr mn degT m/s\n"
            "2021 12 31 23 00  180  5.5\n"
        )
        rt_body = (
            "#YY  MM DD hh mm WDIR WSPD PTDY\n"
            "#yr  mo dy hr mn degT m/s  hPa\n"
            "2022 01 01 00 00  190  6.0 -1.2\n"
        )
        rows = StdmetParser.parse_responses(
            [{'status': 200, 'body': hist_body}, {'status': 200, 'body': rt_body}],
            use_timestamp=True,
        )
        assert len(rows) == 2
        assert 'PTDY' in rows[0]
        assert rows[0]['PTDY'] is None
        assert rows[1]['PTDY'] == -1.2

        # Selecting PTDY via handle_data must succeed even though row 0 originally lacked it
        df = handle_data(rows, as_df=True, cols=['PTDY'])
        assert 'PTDY' in df.columns
        assert len(df) == 2


class TestSpatialZeroAndCoordParsing:
    """Verify lat=0.0 / lon=0.0 and flexible coordinate string formats."""

    def test_lat_lon_map_variants_and_bounds(self):
        assert StationsHandler.LAT_MAP('38.88N') == pytest.approx(38.88)
        assert StationsHandler.LAT_MAP('38.88n') == pytest.approx(38.88)
        assert StationsHandler.LAT_MAP('  12.5°S ') == pytest.approx(-12.5)
        assert StationsHandler.LAT_MAP('-12.5') == pytest.approx(-12.5)
        assert StationsHandler.LAT_MAP('0.0N') == pytest.approx(0.0)

        assert StationsHandler.LON_MAP('76.43W') == pytest.approx(-76.43)
        assert StationsHandler.LON_MAP('76.43w') == pytest.approx(-76.43)
        assert StationsHandler.LON_MAP('140.2°E') == pytest.approx(140.2)
        assert StationsHandler.LON_MAP('0.0W') == pytest.approx(0.0)

        with pytest.raises(ValueError):
            StationsHandler.LAT_MAP('95.0N')
        with pytest.raises(ValueError):
            StationsHandler.LON_MAP('370.0E')
        with pytest.raises(ValueError):
            StationsHandler.LAT_MAP('')

    def test_equator_prime_meridian_zero_float_accepted(self, monkeypatch):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        mock_stations = [
            {'Station': 'eq001', 'Lat': '0.0N', 'Lon': '0.0E'},
            {'Station': 'tplm2', 'Lat': '38.88N', 'Lon': '76.43W'},
        ]
        monkeypatch.setattr(api, 'stations', lambda as_df=False, as_pl=False: mock_stations)
        assert api.nearest_station(lat=0.0, lon=0.0) == 'eq001'
        rad_df = api.radial_search(lat=0.0, lon=0.0, radius=100, units='km')
        assert len(rad_df) == 1
        assert rad_df.iloc[0]['Station'] == 'eq001'


# ===========================================================================
# Tier 2: Cache Lifecycle, Concurrency, OpenDAP, Logging
# ===========================================================================


class TestCacheAndLifecycleRigor:
    """Verify clear_cache, load_cache, dynamic set_cache_limit, and context manager."""

    def test_clear_cache_actually_empties_stations(self):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        api._handler.get_station('test_stn').reqs.put('https://example.com/1', {'status': 200, 'body': 'ok'})
        assert len(api.dump_cache()) > 0
        api.clear_cache()
        assert api.dump_cache() == {}
        assert api._handler.stations == []

    def test_dump_and_load_cache_roundtrip(self, tmp_path):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        api.clear_cache()
        sample_cache = {
            'tplm2': {
                'https://www.ndbc.noaa.gov/data/realtime2/TPLM2.txt': {
                    'status': 200,
                    'body': 'cached_body',
                }
            }
        }
        api.load_cache(sample_cache)
        assert api.dump_cache() == sample_cache

        # Round-trip via pickle file
        cache_file = tmp_path / 'cache.pkl'
        api.dump_cache(dest_fp=cache_file)
        api.clear_cache()
        assert api.dump_cache() == {}
        api.load_cache(cache_file)
        assert api.dump_cache() == sample_cache

        with pytest.raises(ValueError):
            api.load_cache(12345)

    @pytest.mark.asyncio
    async def test_async_load_cache_roundtrip(self, tmp_path):
        async with AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000) as api:
            sample_cache = {
                '41001': {
                    'https://www.ndbc.noaa.gov/data/realtime2/41001.txt': {
                        'status': 200,
                        'body': 'async_cached',
                    }
                }
            }
            api.load_cache(sample_cache)
            assert api.dump_cache() == sample_cache

            cache_file = tmp_path / 'async_cache.pkl'
            api.dump_cache(dest_fp=cache_file)
            api.clear_cache()
            assert api.dump_cache() == {}
            api.load_cache(cache_file)
            assert api.dump_cache() == sample_cache

    def test_request_cache_resize_and_zero_capacity(self):
        cache = RequestCache(capacity=3)
        cache.put('u1', {'status': 200, 'body': '1'})
        cache.put('u2', {'status': 200, 'body': '2'})
        cache.put('u3', {'status': 200, 'body': '3'})
        assert len(cache.cache) == 3

        # Shrink capacity to 1 -> evicts u1 and u2, keeps u3
        cache.set_capacity(1)
        assert len(cache.cache) == 1
        assert cache.get('u3') == {'status': 200, 'body': '3'}
        assert cache.get('u1') == {}

        # Capacity 0 -> safely evicts without corrupting sentinel pointers
        cache.set_capacity(0)
        assert len(cache.cache) == 0
        cache.put('u4', {'status': 200, 'body': '4'})
        assert len(cache.cache) == 0

    def test_sync_context_manager(self):
        with NdbcApi(logging_level=logging.DEBUG, cache_limit=10000) as api:
            assert isinstance(api, NdbcApi)

    def test_log_formatter_does_not_duplicate_dict(self):
        formatter = LogFormatter('[%(levelname)s]: %(message)s')
        record = logging.LogRecord(
            name='NdbcApi',
            level=logging.DEBUG,
            pathname=__file__,
            lineno=1,
            msg={'station_id': 'tplm2', 'message': 'hello'},
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert formatted.count('tplm2') == 1
        # Original record.msg remains a dict
        assert isinstance(record.msg, dict)

    def test_opendap_concat_sorts_non_monotonic_time(self):
        ds_late = xarray.Dataset(
            {'wspd': (['time'], [5.0, 6.0])},
            coords={'time': pd.to_datetime(['2022-01-03', '2022-01-04'])},
        )
        ds_early = xarray.Dataset(
            {'wspd': (['time'], [3.0, 4.0])},
            coords={'time': pd.to_datetime(['2022-01-01', '2022-01-02'])},
        )
        combined = concat_datasets([ds_late, ds_early], temporal_dim_name='time')
        sliced = filter_dataset_by_time_range(
            combined,
            start_time=datetime(2022, 1, 2),
            end_time=datetime(2022, 1, 3),
        )
        assert list(sliced['wspd'].values) == [4.0, 5.0]


# ===========================================================================
# Tier 3: Researcher-Empowering Extensions
# ===========================================================================


class TestResearcherExtensions:
    """Verify describe_mode, df.attrs['units'], timestamps, legacy files, and py.typed."""

    def test_describe_mode_sync_and_async(self):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        all_modes = api.describe_mode()
        assert 'stdmet' in all_modes
        assert 'cwind' in all_modes

        stdmet_info = api.describe_mode('stdmet')
        assert stdmet_info['mode'] == 'stdmet'
        assert stdmet_info['columns']['WVHT']['units'] == 'm'
        assert stdmet_info['columns']['PRES']['units'] == 'hPa'

        async_api = AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        assert async_api.describe_mode('cwind')['columns']['WSPD']['units'] == 'm/s'

        with pytest.raises(RequestException):
            api.describe_mode('nonexistent_mode')

    def test_dataframe_units_attached_to_attrs(self):
        body = (
            "#YY  MM DD hh mm WDIR WSPD WVHT PRES\n"
            "#yr  mo dy hr mn degT m/s     m  hPa\n"
            "2022 01 01 00 00  180  7.5  1.8 1013.2\n"
        )
        rows = StdmetParser.parse_responses([{'status': 200, 'body': body}], use_timestamp=True)
        df = handle_data(rows, as_df=True)
        assert 'units' in df.attrs
        assert df.attrs['units']['WSPD'] == 'm/s'
        assert df.attrs['units']['WVHT'] == 'm'
        assert df.attrs['units']['PRES'] == 'hPa'

    def test_handle_timestamp_rich_inputs(self):
        expected_day = datetime(2022, 8, 15, 0, 0)
        assert handle_timestamp('2022-08-15') == expected_day
        assert handle_timestamp(date(2022, 8, 15)) == expected_day
        assert handle_timestamp(pd.Timestamp('2022-08-15')) == expected_day

        # ISO-8601 with time and UTC 'Z'
        assert handle_timestamp('2022-08-15T14:30:00Z') == datetime(2022, 8, 15, 14, 30)

        # Timezone-aware offset normalized to naive UTC
        edt = timezone(timedelta(hours=-4))
        aware_dt = datetime(2022, 8, 15, 10, 30, tzinfo=edt)
        assert handle_timestamp(aware_dt) == datetime(2022, 8, 15, 14, 30)
        assert handle_timestamp('2022-08-15T10:30:00-04:00') == datetime(2022, 8, 15, 14, 30)

        with pytest.raises(TimestampException):
            handle_timestamp(pd.NaT)
        with pytest.raises(TimestampException):
            handle_timestamp(12345)

    def test_legacy_4_col_and_2_digit_year_historical_parsing(self):
        legacy_1998_body = (
            "YY MM DD hh WD   WSPD GST  WVHT  DPD   APD  MWD  BAR    ATMP  WTMP  DEWP  VIS\n"
            "98 05 15 12 180  6.2  7.8  1.20  8.00  5.50 175 1015.2  18.4  17.1  14.2 99.0\n"
            "04 11 02 06 099  8.1  9.5  2.10 10.00  7.20 099  999.0  12.1  14.0   9.8 99.0\n"
        )
        rows = StdmetParser.parse_responses(
            [{'status': 200, 'body': legacy_1998_body}],
            use_timestamp=True,
        )
        assert len(rows) == 2
        assert rows[0]['timestamp'] == datetime(1998, 5, 15, 12, 0)
        assert rows[0]['WD'] == 180
        assert rows[0]['BAR'] == 1015.2
        assert rows[0]['VIS'] is None

        assert rows[1]['timestamp'] == datetime(2004, 11, 2, 6, 0)
        assert rows[1]['WD'] == 99
        assert rows[1]['MWD'] == 99
        assert rows[1]['BAR'] == 999.0

    def test_df_from_responses_backward_compatibility(self, read_responses):
        assert hasattr(BaseParser, 'df_from_responses')
        assert hasattr(ActiveStationsParser, 'df_from_responses')
        assert hasattr(HistoricalStationsParser, 'df_from_responses')

        stn_resp = list(read_responses['stations'].values())[0]
        stn_df = ActiveStationsParser.df_from_responses(stn_resp, use_timestamp=False)
        assert isinstance(stn_df, pd.DataFrame)
        assert not stn_df.empty

    def test_pep561_py_typed_marker_exists(self):
        py_typed = Path(__file__).resolve().parent.parent / 'ndbc_api' / 'py.typed'
        assert py_typed.is_file()


# ===========================================================================
# Tier 4: Systems Architecture & Researcher-Experience (RX) Rigor
# ===========================================================================


class TestUnifiedDispatchAndBuildRequests:
    """Verify HTTP/OpenDAP dispatch registries and dry-run build_requests."""

    def test_dispatch_registry_coverage(self):
        expected_http = {
            'adcp', 'cwind', 'ocean', 'spec', 'stdmet',
            'supl', 'swden', 'swdir', 'swdir2', 'swr1', 'swr2',
        }
        expected_opendap = {
            'adcp', 'cwind', 'hfradar', 'ocean', 'pwind', 'stdmet', 'swden', 'wlevel',
        }
        assert set(HTTP_DISPATCH.keys()) == expected_http
        assert set(OPENDAP_DISPATCH.keys()) == expected_opendap

        for mode in expected_http:
            assert hasattr(DataHandler, mode)
        for mode in expected_opendap:
            assert hasattr(OpenDapDataHandler, mode)

        with pytest.raises(RequestException):
            build_mode_requests(
                mode='invalid_mode',
                station_id='tplm2',
                start_time=datetime(2022, 1, 1),
                end_time=datetime(2022, 1, 2),
            )

    def test_build_requests_sync_and_async(self, monkeypatch):
        monkeypatch.setenv('MOCKDATE', '2022-08-13')
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)

        # Single station, single mode (HTTP) spanning into the 45-day realtime window
        urls = api.build_requests(
            station_id='tplm2',
            mode='stdmet',
            start_time='2022-01-01',
            end_time='2022-08-10',
        )
        assert isinstance(urls, list)
        assert any('tplm212022.txt.gz' in u for u in urls)
        assert any('TPLM2.txt' in u for u in urls)

        # Multi-station, multi-mode (OpenDAP)
        od_urls = api.build_requests(
            station_ids=['tplm2', '41001'],
            modes=['stdmet', 'cwind'],
            start_time='2021-01-01',
            end_time='2021-06-01',
            as_xarray_dataset=True,
        )
        assert isinstance(od_urls, dict)
        assert set(od_urls.keys()) == {'tplm2', '41001'}
        assert set(od_urls['tplm2'].keys()) == {'stdmet', 'cwind'}
        assert any('thredds/fileServer/data/stdmet/tplm2/tplm2h2021.nc' in u for u in od_urls['tplm2']['stdmet'])

        # AsyncNdbcApi parity
        async_api = AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        async_urls = async_api.build_requests(
            station_id='tplm2',
            mode='stdmet',
            start_time='2022-01-01',
            end_time='2022-08-10',
        )
        assert async_urls == urls

        with pytest.raises(ValueError):
            api.build_requests(mode='stdmet')
        with pytest.raises(ValueError):
            api.build_requests(station_id='tplm2')
        with pytest.raises(RequestException):
            api.build_requests(station_id='tplm2', mode='not_a_mode')


class TestSingletonReconfiguration:
    """Verify NdbcApi singleton updates explicitly passed parameters."""

    def test_singleton_reconfigures_when_explicit_kwargs_passed(self):
        api1 = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        try:
            api2 = NdbcApi(
                cache_limit=256,
                delay=12,
                retries=4,
                backoff_factor=0.25,
                headers={'X-Custom-Agent': 'ResearchTest'},
            )
            assert api1 is api2
            assert api2.cache_limit == 256
            assert api2._delay == 12
            assert api2._retries == 4
            assert api2._backoff_factor == 0.25
            assert api2.get_headers() == {'X-Custom-Agent': 'ResearchTest'}

            # Zero-arg call preserves existing singleton configuration
            api3 = NdbcApi()
            assert api3 is api1
            assert api3.cache_limit == 256
            assert api3.get_headers() == {'X-Custom-Agent': 'ResearchTest'}
        finally:
            # Always restore test suite default configuration
            NdbcApi(
                logging_level=logging.DEBUG,
                cache_limit=10000,
                delay=5,
                retries=5,
                backoff_factor=0.8,
                headers={},
            )


class TestTemporalRangeValidation:
    """Verify inverted start_time > end_time raises TimestampException immediately."""

    def test_enforce_timerange_rejects_inverted_window(self):
        df = pd.DataFrame(
            {'WSPD': [5.0]},
            index=pd.DatetimeIndex([datetime(2022, 1, 1)], name='timestamp'),
        )
        with pytest.raises(TimestampException, match='must be <='):
            enforce_timerange(
                df,
                start_time=datetime(2022, 6, 1),
                end_time=datetime(2022, 1, 1),
            )

    def test_get_data_and_build_requests_reject_inverted_window(self):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        with pytest.raises(TimestampException, match='must be <='):
            api.get_data(
                station_id='tplm2',
                mode='stdmet',
                start_time='2022-06-01',
                end_time='2022-01-01',
            )
        with pytest.raises(TimestampException, match='must be <='):
            api.build_requests(
                station_id='tplm2',
                mode='stdmet',
                start_time='2022-06-01',
                end_time='2022-01-01',
            )

    @pytest.mark.asyncio
    async def test_async_get_data_rejects_inverted_window(self):
        async with AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000) as api:
            with pytest.raises(TimestampException, match='must be <='):
                await api.get_data(
                    station_id='tplm2',
                    mode='stdmet',
                    start_time='2022-06-01',
                    end_time='2022-01-01',
                )


class TestStationAndTabularParserHardening:
    """Verify HTML/XML/tabular parsers handle non-5-char IDs, missing tags, and tabs."""

    def test_historical_parser_6char_id_and_nested_anchor_href(self):
        html = """
        <html><body>
        <p>Available historical data for station nblp4v include:</p>
        <ul>
          <li>Standard Meteorological Data:<br>
            <ul>
              <li><b>Historical:</b> <a href="/view_text_file.php?filename=nblp4vh2021.txt.gz">2021</a></li>
            </ul>
          </li>
        </ul>
        </body></html>
        """
        parsed = HistoricalParser.available_measurements({'status': 200, 'body': html})
        assert 'Historical' in parsed
        assert parsed['Historical']['2021'].endswith('/view_text_file.php?filename=nblp4vh2021.txt.gz')

    def test_historical_parser_missing_section_returns_empty_dict(self):
        html = '<html><body><h1>Station 41001</h1><p>No historical data.</p></body></html>'
        assert HistoricalParser.available_measurements({'status': 200, 'body': html}) == {}

    def test_metadata_parser_multi_colon_values_and_missing_h1(self):
        html = (
            '<html><body>'
            '<h1>Station TPLM2 - Thomas Point</h1>'
            '<div id="stn_metadata"><p>'
            'Owned and maintained by National Data Buoy Center\n'
            '38.88N 76.43W\n\n'
            'Site elevation: sea level: datum: MLLW\n'
            'Air temp height: 4 m above site elevation'
            '</p></div>'
            '</body></html>'
        )
        meta = MetadataParser.metadata({'status': 200, 'body': html})
        assert meta['Name'] == 'Station TPLM2 - Thomas Point'
        assert meta['Site elevation'] == 'sea level: datum: MLLW'
        assert meta['Location'] == '38.88N 76.43W'

        empty_html = '<html><body><div>No h1 here</div></body></html>'
        assert MetadataParser.metadata({'status': 200, 'body': empty_html}) == {}

    def test_xml_station_parsers_tolerate_missing_lat_lon_and_catch_bad_xml(self):
        active_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <stations created="2022-08-13T00:00:00UTC" count="2">
          <station id="tplm2" lat="38.88" lon="-76.43" name="Thomas Point" owner="NDBC" pgm="NDBC" type="fixed" met="y" currents="n" waterquality="n" dart="n" />
          <station id="nocrd" lat="" lon="" name="Unplaced Buoy" owner="NDBC" />
        </stations>
        """
        df_active = ActiveStationsParser.df_from_responses({'status': 200, 'body': active_xml})
        assert len(df_active) == 2
        assert pd.isna(df_active.loc[df_active['Station'] == 'nocrd', 'Lat'].iloc[0])

        hist_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <stations>
          <station id="old01" name="Old Buoy">
            <history start="1980-01-01" stop="1985-01-01" lat="" lng="" elev="" met="y" hull="3D" anemo="4.9" />
          </station>
        </stations>
        """
        df_hist = HistoricalStationsParser.df_from_responses({'status': 200, 'body': hist_xml})
        assert len(df_hist) == 1
        assert pd.isna(df_hist.iloc[0]['Lat'])

        with pytest.raises(ParserException):
            ActiveStationsParser.df_from_responses({'status': 200, 'body': '<unclosed>'})
        with pytest.raises(ParserException):
            HistoricalStationsParser.df_from_responses({'status': 200, 'body': '<unclosed>'})

    def test_tab_delimited_tabular_response(self):
        tab_body = (
            "#YY\tMM\tDD\thh\tmm\tWDIR\tWSPD\n"
            "#yr\tmo\tdy\thr\tmn\tdegT\tm/s\n"
            "2022\t01\t01\t00\t00\t180\t6.4\n"
        )
        rows = StdmetParser.parse_responses([{'status': 200, 'body': tab_body}], use_timestamp=True)
        assert len(rows) == 1
        assert rows[0]['WDIR'] == 180
        assert rows[0]['WSPD'] == 6.4

    def test_top_level_package_exports(self):
        assert issubclass(TimestampException, NdbcException)
        assert issubclass(RequestException, NdbcException)
        assert issubclass(ResponseException, NdbcException)
        assert issubclass(ParserException, NdbcException)
        assert issubclass(HandlerException, NdbcException)
        assert callable(describe_mode)
        assert 'stdmet' in MODE_METADATA
        assert COLUMN_UNITS['WVHT'] == 'm'


# ===========================================================================
# Tier 5: High-Conviction Refinements & Researcher Capabilities
# ===========================================================================


class TestHighConvictionRefinementsAndExtensions:
    """Verify mode extraction, scalar-dict DataFrames, multi-station OpenDAP, spatial filters, deployments, and explain_station."""

    def test_extract_realtime_modes_detects_all_spectral_and_met_modes(self, read_parsed_yml):
        rt_yml = read_parsed_yml['realtime']
        supported = list(HTTP_DISPATCH.keys())
        want = ['spec', 'stdmet', 'supl', 'swden', 'swdir', 'swdir2', 'swr1', 'swr2']
        got = extract_realtime_modes(rt_yml, supported)
        assert want == got

        # Also handles entries missing 'description' key without raising KeyError
        minimal = {
            'Custom wind': {
                'Custom wind': 'https://www.ndbc.noaa.gov/data/realtime2/TPLM2.cwind',
            }
        }
        want_minimal = ['cwind']
        got_minimal = extract_realtime_modes(minimal, supported)
        assert want_minimal == got_minimal

    def test_summarize_historical_modes_maps_canonical_names(self, read_parsed_yml):
        hist_yml = read_parsed_yml['historical']
        supported = list(HTTP_DISPATCH.keys())
        got = summarize_historical_modes(hist_yml, supported)
        want_keys = {'cwind', 'stdmet'}
        assert want_keys == set(got.keys())
        assert '1990' in got['cwind']
        assert '1985' in got['stdmet']

    def test_station_metadata_as_df_returns_single_row_dataframe(self):
        scalar_meta = {
            'Name': 'Station TPLM2 - Thomas Point',
            'Location': '38.88N 76.43W',
            'Site elevation': 'sea level',
        }
        df = handle_data(scalar_meta, as_df=True)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]['Name'] == 'Station TPLM2 - Thomas Point'
        assert df.iloc[0]['Site elevation'] == 'sea level'

    def test_multi_station_opendap_preserves_station_id_dimension(self):
        ds_tplm2 = xarray.Dataset(
            {'wind_spd': (['time'], [5.0, 6.0])},
            coords={
                'time': pd.to_datetime(['2022-01-01', '2022-01-02']),
                'station_id': 'tplm2',
            },
        )
        ds_41001 = xarray.Dataset(
            {'wind_spd': (['time'], [12.0, 14.0])},
            coords={
                'time': pd.to_datetime(['2022-01-01', '2022-01-02']),
                'station_id': '41001',
            },
        )
        merged = merge_datasets([ds_tplm2, ds_41001])
        assert 'station_id' in merged.dims
        assert list(merged.sel(station_id='tplm2')['wind_spd'].values) == [5.0, 6.0]
        assert list(merged.sel(station_id='41001')['wind_spd'].values) == [12.0, 14.0]

    def test_polars_multi_station_deterministic_sort(self):
        accumulated = {
            'stdmet': [
                [{'timestamp': datetime(2022, 1, 1, 0, 0), 'station_id': 'tplm2', 'WSPD': 5.0}],
                [{'timestamp': datetime(2022, 1, 1, 0, 0), 'station_id': '41001', 'WSPD': 9.0}],
            ]
        }
        pl_df = handle_accumulate_data(accumulated, as_df=False, as_pl=True)
        assert list(pl_df['station_id']) == ['41001', 'tplm2']

    def test_capability_filtered_nearest_and_radial_search(self, monkeypatch):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        catalog = [
            {
                'Station': 'dart1',
                'Lat': 38.88,
                'Lon': -76.43,
                'Includes Meteorology': False,
                'Includes Currents': False,
                'Includes Water Quality': False,
                'DART Program': True,
                'Owner': 'NDBC',
                'Program': 'Tsunami',
                'Type': 'dart',
            },
            {
                'Station': 'tplm2',
                'Lat': 38.90,
                'Lon': -76.44,
                'Includes Meteorology': True,
                'Includes Currents': False,
                'Includes Water Quality': False,
                'DART Program': False,
                'Owner': 'NDBC',
                'Program': 'C-MAN',
                'Type': 'fixed',
            },
            {
                'Station': 'adcp1',
                'Lat': 38.95,
                'Lon': -76.50,
                'Includes Meteorology': True,
                'Includes Currents': True,
                'Includes Water Quality': True,
                'DART Program': False,
                'Owner': 'NOS',
                'Program': 'IOOS',
                'Type': 'buoy',
            },
        ]
        monkeypatch.setattr(api, 'stations', lambda as_df=False, as_pl=False: catalog)

        # Unfiltered nearest is dart1
        assert api.nearest_station(lat=38.88, lon=-76.43) == 'dart1'
        # Filtered by met=True skips dart1 and returns tplm2
        assert api.nearest_station(lat=38.88, lon=-76.43, met=True) == 'tplm2'
        # Filtered by currents=True returns adcp1
        assert api.nearest_station(lat=38.88, lon=-76.43, currents=True) == 'adcp1'
        # Top-2 met stations returns ['tplm2', 'adcp1']
        assert api.nearest_station(lat=38.88, lon=-76.43, met=True, k=2) == ['tplm2', 'adcp1']

        # Radial search with met=True and limit=1
        rad = api.radial_search(lat=38.88, lon=-76.43, radius=50, units='km', met=True, limit=1)
        assert len(rad) == 1
        assert rad.iloc[0]['Station'] == 'tplm2'

        with pytest.raises(ValueError):
            api.nearest_station(lat=38.88, lon=-76.43, k=0)
        with pytest.raises(ValueError):
            api.radial_search(lat=38.88, lon=-76.43, radius=50, limit=0)

    def test_station_deployments_and_explain_station_sync_and_async(self, monkeypatch):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        deployments = [
            {
                'Station': '41001',
                'Lat': 34.68,
                'Lon': -72.66,
                'Elevation': 0.0,
                'Name': 'East Hatteras',
                'Hull Type': '6N',
                'Anemometer Height': 5.0,
                'Start Date': '1995-01-01',
                'End Date': '2005-12-31',
            },
            {
                'Station': '41001',
                'Lat': 34.70,
                'Lon': -72.70,
                'Elevation': 0.0,
                'Name': 'East Hatteras',
                'Hull Type': '3D',
                'Anemometer Height': 4.1,
                'Start Date': '2006-01-01',
                'End Date': '',
            },
            {
                'Station': 'tplm2',
                'Lat': 38.88,
                'Lon': -76.43,
                'Elevation': 0.0,
                'Name': 'Thomas Point',
                'Hull Type': 'CMAN',
                'Anemometer Height': 18.0,
                'Start Date': '1985-01-01',
                'End Date': '',
            },
        ]
        monkeypatch.setattr(api, 'historical_stations', lambda as_df=False, as_pl=False: deployments)
        df_all = api.station_deployments('41001', as_df=True)
        assert len(df_all) == 2

        df_recent = api.station_deployments('41001', start_time='2015-01-01', end_time='2020-01-01')
        assert len(df_recent) == 1
        assert df_recent.iloc[0]['Anemometer Height'] == pytest.approx(4.1)

        with pytest.raises(TimestampException):
            filter_station_deployments(deployments, '41001', start_time='2022-01-01', end_time='2020-01-01')

        # explain_station synthesis
        monkeypatch.setattr(api, 'station', lambda station_id, as_df=False, as_pl=False: {'Name': 'Thomas Point'})
        monkeypatch.setattr(api, 'available_realtime', lambda station_id, full_response=False: ['cwind', 'stdmet'])
        monkeypatch.setattr(
            api,
            'available_historical',
            lambda station_id, full_response=True, as_df=False, as_pl=False: {'stdmet': ['2020', '2021']},
        )
        want = {
            'station_id': 'tplm2',
            'metadata': {'Name': 'Thomas Point'},
            'realtime_modes': ['cwind', 'stdmet'],
            'historical_modes': {'stdmet': ['2020', '2021']},
        }
        got = api.explain_station('TPLM2')
        assert want == got

    @pytest.mark.asyncio
    async def test_coordinate_bounds_validation_sync_and_async_before_network(self, monkeypatch):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)

        def fail_if_called(*args, **kwargs):
            raise AssertionError('stations() should not be called when coordinates are invalid')

        monkeypatch.setattr(api, 'stations', fail_if_called)
        for bad_lat, bad_lon in [(95.0, 0.0), (0.0, 370.0), (float('nan'), 0.0), ('95.0N', '0.0E')]:
            with pytest.raises(ValueError):
                api.nearest_station(lat=bad_lat, lon=bad_lon)
            with pytest.raises(ValueError):
                api.radial_search(lat=bad_lat, lon=bad_lon, radius=10)

        async with AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000) as async_api:
            monkeypatch.setattr(async_api, 'stations', fail_if_called)
            for bad_lat, bad_lon in [(95.0, 0.0), (0.0, 370.0), (float('nan'), 0.0), ('95.0N', '0.0E')]:
                with pytest.raises(ValueError):
                    await async_api.nearest_station(lat=bad_lat, lon=bad_lon)
                with pytest.raises(ValueError):
                    await async_api.radial_search(lat=bad_lat, lon=bad_lon, radius=10)

    def test_vestige_aliases_and_mode_list_parity(self):
        api = NdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        async_api = AsyncNdbcApi(logging_level=logging.DEBUG, cache_limit=10000)
        assert api.get_modes(use_opendap=False) == async_api.get_modes(use_opendap=False)
        assert api.get_modes(use_opendap=True) == async_api.get_modes(use_opendap=True)
        assert hasattr(BaseParser, '_read_response')
        assert hasattr(BaseParser, '_read_response_fallback')
        assert hasattr(MetadataParser, '_meta_from_response')
        assert hasattr(MetadataParser, '_meta_from_respose')




