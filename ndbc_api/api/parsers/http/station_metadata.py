from collections import ChainMap
from typing import List

import bs4

from ndbc_api.api.parsers.http._station import StationParser


class _StationMetadataDict(dict):
    """Backward-compatible station metadata dictionary.

    Preserves the historical `'Statation Type'` key so equality checks and
    existing callers remain 100% intact, while transparently resolving
    `'Station Type'` lookups (`d['Station Type']`, `d.get('Station Type')`,
    `'Station Type' in d`).
    """

    def __getitem__(self, key):
        if (
            key == 'Station Type'
            and not super().__contains__('Station Type')
            and super().__contains__('Statation Type')
        ):
            return super().__getitem__('Statation Type')
        return super().__getitem__(key)

    def get(self, key, default=None):
        if (
            key == 'Station Type'
            and not super().__contains__('Station Type')
            and super().__contains__('Statation Type')
        ):
            return super().get('Statation Type', default)
        return super().get(key, default)

    def __contains__(self, key):
        if key == 'Station Type' and super().__contains__('Statation Type'):
            return True
        return super().__contains__(key)


class MetadataParser(StationParser):

    @classmethod
    def metadata(cls, response: dict) -> dict:
        if response.get('status') == 200:
            soup = bs4.BeautifulSoup(response.get('body') or '', 'html.parser')
            metadata = cls._meta_from_response(soup=soup)
            return _StationMetadataDict(ChainMap(*metadata))
        else:
            return _StationMetadataDict()

    """ PRIVATE """

    @classmethod
    def _meta_from_response(cls, soup: bs4.BeautifulSoup):
        metadata = []
        try:
            metadata.append({'Name': soup.find('h1').text.strip()})
            items = soup.find('div', id='stn_metadata').find_all('p')[0].text
            items = items.split('\n\n')
            assert len(items) == 2
        except (AssertionError, AttributeError, IndexError):
            return metadata
        metadata.extend(cls._parse_headers(items[0]))
        metadata.extend(cls._parse_attrs(items[1]))
        return metadata

    _meta_from_respose = _meta_from_response

    @classmethod
    def _parse_headers(cls, line_meta):
        station_headers = []
        headers = [i.strip() for i in line_meta.split('\n') if i.strip()]
        if not headers:
            return station_headers
        station_headers.append({'Statation Type': ', '.join(headers[0:-1])})
        station_headers.append({'Location': headers[-1]})
        return station_headers

    @classmethod
    def _parse_attrs(cls, line_attr: str) -> List[dict]:
        station_attrs = []
        attrs = [i for i in line_attr.split('\n') if i]
        for attr in attrs:
            if ': ' in attr:
                k, v = attr.split(': ', 1)
                station_attrs.append({k: v})
        return station_attrs
