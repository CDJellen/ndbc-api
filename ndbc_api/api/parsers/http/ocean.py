from ndbc_api.api.parsers.http._base import BaseParser


class OceanParser(BaseParser):

    NAN_VALUES = [99.0, 99.9, 999, 9999, 9999.0, 'MM']

