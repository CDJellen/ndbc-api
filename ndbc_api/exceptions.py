class NdbcException(Exception):
    """Base exception that all other NDBC exceptions subclass from."""

    def __init__(self, message: str = ''):  # pragma: no cover
        self.message = message
        super().__init__(self.message)

    def __str__(self):  # pragma: no cover
        return f"NDBC API: {self.message or 'unspecified error'}"


class TimestampException(NdbcException):
    """Unable to handle given timestamp."""


class RequestException(NdbcException):
    """Unable to build the given request."""


class ResponseException(NdbcException):
    """Unable to handle the given response."""


class ParserException(NdbcException):
    """Unable to parse the given response."""


class HandlerException(NdbcException):
    """Error when handling this API call."""
