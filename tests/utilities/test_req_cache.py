import pytest

from ndbc_api.utilities.req_cache import RequestCache


@pytest.fixture(scope='session')
def request_handler():
    yield RequestCache(capacity=2)


@pytest.fixture(scope='session')
def response_foo():
    yield {'status': 200, 'body': 'foo'}


@pytest.fixture(scope='session')
def response_bar():
    yield {'status': 200, 'body': 'bar'}


@pytest.fixture(scope='session')
def response_baz():
    yield {'status': 404, 'body': 'baz'}


@pytest.fixture(scope='session')
def response_foobar():
    yield {'status': 200, 'body': 'foobar'}


def test_request_cache(request_handler):
    assert request_handler.capacity == 2
    assert len(request_handler.cache) == 0
    assert isinstance(request_handler.cache, dict)


def test_request_cache_put(request_handler, response_foo):
    request_handler.put(request='foo', response=response_foo)
    assert len(request_handler.cache) == 1


def test_request_cache_limit(request_handler, response_foo, response_bar, response_baz):
    request_handler.put(request='foo', response=response_foo)
    request_handler.put(request='bar', response=response_bar)
    assert len(request_handler.cache) == 2
    request_handler.put(request='baz', response=response_baz)
    assert len(request_handler.cache) == 2


def test_request_cache_overwrite(request_handler, response_foo, response_foobar):
    request_handler.put(request='foo', response=response_foo)
    want = response_foo
    got = request_handler.get('foo')
    assert got == want
    request_handler.put(request='foo', response=response_foobar)
    want = response_foobar
    got = request_handler.get('foo')
    assert got == want
