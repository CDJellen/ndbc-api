"""
A metaclass for singleton types.
"""

import pprint
from logging import Formatter


class LogFormatter(Formatter):
    """Formatter that pretty-prints dictionaries in log messages."""

    def format(self, record):
        formatted_message = super().format(record)
        if isinstance(record.msg, dict):
            formatted_message += "\n" + pprint.pformat(record.msg)
        return formatted_message
