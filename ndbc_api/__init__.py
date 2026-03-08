"""A Python API for the National Data Buoy Center.

The ``ndbc-api`` package provides two top-level API classes:

* :class:`NdbcApi` — synchronous API (always available).
* :class:`AsyncNdbcApi` — async API backed by ``aiohttp``.
  Requires the ``async`` extra::

      pip install ndbc-api[async]
"""
__docformat__ = "restructuredtext"

from ndbc_api.ndbc_api import NdbcApi

__all__ = ["NdbcApi"]

try:
    from ndbc_api.async_ndbc_api import AsyncNdbcApi

    __all__.append("AsyncNdbcApi")
except ImportError:  # aiohttp not installed
    pass
