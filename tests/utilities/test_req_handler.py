import pytest

from ndbc_api.utilities.req_handler import RequestHandler


@pytest.fixture(scope='module')
def request_handler():
    yield RequestHandler(
        cache_limit=3,
        log=None,
        delay=4,
        retries=8,
        backoff_factor=0.9,
        headers={},
        debug=False,
        verify_https=False,
    )


@pytest.fixture(scope='module')
def request_header_bar():
    yield dict(foo='bar')


@pytest.fixture(scope='module')
def request_header_baz():
    yield dict(foo='baz')


@pytest.mark.private
def test_cache_limit(request_handler):
    assert request_handler.get_cache_limit() > 0


@pytest.mark.private
def test_set_cache_limit(request_handler):
    request_handler.set_cache_limit(6)
    assert request_handler._cache_limit == 6


@pytest.mark.private
def test_set_headers(request_handler, request_header_bar):
    assert isinstance(request_handler._request_headers, dict)
    request_handler.set_headers(request_header_bar)
    assert isinstance(request_handler._request_headers, dict)
    assert len(request_handler._request_headers) == 1
    assert request_handler._request_headers['foo'] == 'bar'


@pytest.mark.private
def test_update_headers(request_handler, request_header_baz):
    request_handler.update_headers(request_header_baz)
    assert isinstance(request_handler._request_headers, dict)
    assert len(request_handler._request_headers) == 1
    assert request_handler._request_headers['foo'] == 'baz'


@pytest.mark.private
def test_add_station(request_handler):
    cur_len = len(request_handler.stations)
    request_handler.add_station('foo')
    assert len(request_handler.stations) == cur_len + 1


@pytest.mark.private
def test_get_station(request_handler):
    request_handler.get_station('foo')
    cur_len = len(request_handler.stations)
    assert isinstance(request_handler.get_station('foo'),
                      RequestHandler.Station)
    request_handler.get_station(101)
    assert len(request_handler.stations) == cur_len + 1
    request_handler.get_station('101')
    assert len(request_handler.stations) == cur_len + 1

    want = request_handler.get_station(101)
    got = request_handler.get_station('101')
    assert want == got
