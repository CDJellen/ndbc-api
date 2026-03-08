"""Unit tests for :mod:`ndbc_api.utilities.async_req_handler`.

Covers the synchronous delegator methods (cache/headers), station
management, cache-hit path, and retry/backoff logic.
"""
import asyncio
import logging

import aiohttp
import pytest
import pytest_asyncio
from aioresponses import aioresponses

from ndbc_api.utilities.async_req_handler import AsyncRequestHandler


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _noop_log(level, **kwargs):
    """Minimal log callable matching the ``AsyncNdbcApi.log`` signature."""
    pass


# ---------------------------------------------------------------------------
# construction & synchronous delegators
# ---------------------------------------------------------------------------


class TestConstruction:
    """Verify construction and cache/header helpers (L88-106)."""

    def test_defaults(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        assert handler.get_cache_limit() == 10
        assert handler.get_headers() == {}
        assert handler.stations == []

    def test_set_cache_limit(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        handler.set_cache_limit(42)
        assert handler.get_cache_limit() == 42

    def test_get_set_headers(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
            headers={'X-Init': 'yes'},
        )
        assert handler.get_headers() == {'X-Init': 'yes'}
        handler.update_headers({'X-Extra': 'val'})
        assert handler.get_headers() == {'X-Init': 'yes', 'X-Extra': 'val'}
        handler.set_headers({'X-New': 'only'})
        assert handler.get_headers() == {'X-New': 'only'}


# ---------------------------------------------------------------------------
# station management
# ---------------------------------------------------------------------------


class TestStationManagement:
    """Verify has_station / get_station (L108-130)."""

    def test_has_station_false_then_true(self):
        handler = AsyncRequestHandler(
            cache_limit=5, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        assert handler.has_station('tplm2') is False
        handler.get_station('tplm2')
        assert handler.has_station('tplm2') is True

    def test_get_station_int_coercion(self):
        handler = AsyncRequestHandler(
            cache_limit=5, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        stn = handler.get_station(41001)
        assert stn.id_ == '41001'

    def test_get_station_returns_same_instance(self):
        handler = AsyncRequestHandler(
            cache_limit=5, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        stn1 = handler.get_station('tplm2')
        stn2 = handler.get_station('tplm2')
        assert stn1 is stn2


# ---------------------------------------------------------------------------
# cache-hit path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCacheHit:
    """Verify that a second handle_request for the same URL is cached (L179)."""

    async def test_cache_hit_does_not_refetch(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        url = 'https://www.ndbc.noaa.gov/test'
        async with handler:
            with aioresponses() as m:
                m.get(url, status=200, body='first-call')
                resp1 = await handler.handle_request('tplm2', url)

            # Second call — no mock registered, so network would fail.
            # The cache-hit branch (L179) avoids the fetch entirely.
            resp2 = await handler.handle_request('tplm2', url)
            assert resp1 == resp2
            assert resp1['body'] == 'first-call'


# ---------------------------------------------------------------------------
# retry / backoff
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestRetryBackoff:
    """Verify retry logic and exhausted-retries fallback (L219-236)."""

    async def test_retry_then_success(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=2, backoff_factor=0.01,
        )
        url = 'https://www.ndbc.noaa.gov/retry-test'
        async with handler:
            with aioresponses() as m:
                # First attempt: connection error → retry
                m.get(url, exception=aiohttp.ClientError('boom'))
                # Second attempt: success
                m.get(url, status=200, body='ok')
                resp = await handler.execute_request(
                    station_id='tplm2', url=url, headers={})
                assert resp['status'] == 200
                assert resp['body'] == 'ok'

    async def test_all_retries_exhausted(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=1, backoff_factor=0.01,
        )
        url = 'https://www.ndbc.noaa.gov/fail-test'
        async with handler:
            with aioresponses() as m:
                m.get(url, exception=aiohttp.ClientError('fail1'))
                m.get(url, exception=aiohttp.ClientError('fail2'))
                resp = await handler.execute_request(
                    station_id='tplm2', url=url, headers={})
                assert resp['status'] == 0
                assert resp['body'] == ''

    async def test_binary_content_type(self):
        handler = AsyncRequestHandler(
            cache_limit=10, log=_noop_log, delay=0,
            retries=0, backoff_factor=0.1,
        )
        url = 'https://www.ndbc.noaa.gov/binary-test'
        async with handler:
            with aioresponses() as m:
                m.get(url, status=200, body=b'\x00\x01\x02',
                      content_type='application/octet-stream')
                resp = await handler.execute_request(
                    station_id='tplm2', url=url, headers={})
                assert resp['status'] == 200
                assert resp['body'] == b'\x00\x01\x02'
