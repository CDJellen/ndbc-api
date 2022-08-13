from ndbc_api.config import (
    LOGGER_NAME,
    DEFAULT_CACHE_LIMIT,
    HTTP_DEBUG,
    HTTP_DELAY,
    VERIFY_HTTPS,
    HTTP_BACKOFF_FACTOR,
    HTTP_RETRY,
)


def test_logger_name():
    assert isinstance(LOGGER_NAME, str)


def test_default_cache_limit():
    assert isinstance(DEFAULT_CACHE_LIMIT, int)


def test_http_debug_flag():
    assert isinstance(HTTP_DEBUG, bool)


def test_http_delay():
    assert isinstance(HTTP_DELAY, int)


def test_verify_https_flag():
    assert isinstance(VERIFY_HTTPS, bool)


def test_http_backoff_factor():
    assert isinstance(HTTP_BACKOFF_FACTOR, float)


def test_http_retry():
    assert isinstance(HTTP_RETRY, int)
