"""Custom log formatter for the NDBC API."""

import pprint
from logging import Formatter


class LogFormatter(Formatter):
    """Formatter that pretty-prints dictionaries in log messages."""

    def format(self, record):
        orig_msg = record.msg
        try:
            if isinstance(orig_msg, dict):
                record.msg = pprint.pformat(orig_msg)
            return super().format(record)
        finally:
            record.msg = orig_msg
