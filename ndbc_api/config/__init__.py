"""Stores the configuration information for the NDBC API.

Attributes:
    LOGGER_NAME (:str:): The name for the `logging.Logger` in the api instance.
    DEFAULT_CACHE_LIMIT (:int:): The station level limit for caching NDBC data
        service requests.
    VERIFY_HTTPS (:bool:): Whether to execute requests using HTTPS rather than
        HTTP.
    HTTP_RETRY (:int:): The number of times to retry requests to the NDBC data
        service.
    HTTP_BACKOFF_FACTOR (:float:): The backoff factor used when executing retry
        requests to the NDBC data service.
    HTTP_DELAY (:int:) The delay between requests submitted to the NDBC data
        service, in milliseconds.
    HTTP_DEBUG (:bool:): Whether to log requests and responses to the NDBC API's
        log (a `logging.Logger`) as debug messages.
"""
LOGGER_NAME = 'NDBC-API'
DEFAULT_CACHE_LIMIT = 36
VERIFY_HTTPS = True
HTTP_RETRY = 5
HTTP_BACKOFF_FACTOR = 0.8
HTTP_DELAY = 2000
HTTP_DEBUG = True
