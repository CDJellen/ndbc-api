""" .. include:: ../README.md """
__docformat__ = 'restructuredtext'

from ndbc_api.exceptions import (
    HandlerException,
    NdbcException,
    ParserException,
    RequestException,
    ResponseException,
    TimestampException,
)
from ndbc_api.ndbc_api import NdbcApi
from ndbc_api.utilities.mode_metadata import (
    COLUMN_UNITS,
    MODE_METADATA,
    describe_mode,
)

__all__ = [
    'NdbcApi',
    'NdbcException',
    'TimestampException',
    'RequestException',
    'ResponseException',
    'ParserException',
    'HandlerException',
    'describe_mode',
    'MODE_METADATA',
    'COLUMN_UNITS',
]

try:
    from ndbc_api.async_ndbc_api import AsyncNdbcApi as AsyncNdbcApi

    __all__.append('AsyncNdbcApi')
except ImportError:  # aiohttp not installed
    pass
