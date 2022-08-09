import pytest

from ndbc_api.utilities.req_handler import RequestHandler


@pytest.fixture(scope='session')
def request_handler():
    yield RequestHandler(
        cache_limit=3,
        log=None,
        delay=4,
        retries=8,
        backoff_factor=0.9,
        headers={},
        debug=False,
        verify_https=False
    )

@pytest.fixture(scope='session')
def request_header_bar():
    yield dict(foo='bar')

@pytest.fixture(scope='session')
def request_header_baz():
    yield dict(foo='baz')

def test_cache_limit(request_handler):
    assert request_handler.get_cache_limit() == 3

def test_set_cache_limit(request_handler):
    request_handler.set_cache_limit(6)
    assert request_handler._cache_limit == 6

def test_set_headers(request_handler, request_header_bar):
    assert isinstance(request_handler._request_headers, dict)
    request_handler.set_headers(request_header_bar)
    assert isinstance(request_handler._request_headers, dict)
    assert len(request_handler._request_headers) == 1
    assert request_handler._request_headers['foo'] == 'bar'

def test_update_headers(request_handler, request_header_baz):
    request_handler.update_headers(request_header_baz)
    assert isinstance(request_handler._request_headers, dict)
    assert len(request_handler._request_headers) == 1
    assert request_handler._request_headers['foo'] == 'baz'

def test_add_station(request_handler):
    assert len(request_handler.stations) == 0
    request_handler.add_station('foo')
    assert len(request_handler.stations) == 1

def test_get_station(request_handler):
    request_handler.get_station('foo')
    assert len(request_handler.stations) == 1
    assert isinstance(request_handler.get_station('foo'), RequestHandler.Station)
