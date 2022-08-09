import pytest

from ndbc_api.utilities.req_cache import RequestCache


@pytest.fixture(scope='session')
def request_handler():
    yield RequestCache(
        capacity=2
    )

@pytest.fixture(scope='session')
def request_foo():
    yield RequestCache.Request(
        request='foo',
        response={'status': 200, 'body': 'foo'}
    )

@pytest.fixture(scope='session')
def request_bar():
    yield RequestCache.Request(
        request='bar',
        response={'status': 200, 'body': 'bar'}
    )

@pytest.fixture(scope='session')
def request_baz():
    yield RequestCache.Request(
        request='baz',
        response={'status': 200, 'body': 'baz'}
    )

@pytest.fixture(scope='session')
def request_foobar():
    yield RequestCache.Request(
        request='foo',
        response={'status': 200, 'body': 'bar'}
    )

def test_request_cache(request_handler):
    assert request_handler.capacity == 2
    assert len(request_handler.cache) == 0
    assert isinstance(request_handler.cache, dict)

def test_request_cache_add(request_handler, request_foo):
    request_handler.add(request_foo)
    assert len(request_handler.cache) == 1

def test_request_cache_limit(request_handler, request_bar, request_baz):
    request_handler.add(request_bar)
    assert len(request_handler.cache) == 2
    request_handler.add(request_baz)
    assert len(request_handler.cache) == 2

def test_request_cache_limit(request_handler, request_bar, request_baz):
    request_handler.add(request_bar)
    request_handler.add(request_baz)
    assert len(request_handler.cache) == 2
    request_handler.remove(request_baz)
    request_handler.remove(request_bar)
    assert len(request_handler.cache) == 0

def test_request_cache_put():
    pass

def test_request_cache_get():
    pass
