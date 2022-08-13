from collections import ChainMap
from typing import List

import bs4

from ndbc_api.api.parsers._station import StationParser


class MetadataParser(StationParser):
    @classmethod
    def metadata(cls, response: dict) -> dict:
        if response.get('status') == 200:
            soup = bs4.BeautifulSoup(response.get('body'), 'html.parser')
            metadata = cls._meta_from_respose(soup=soup)
            return dict(ChainMap(*metadata))
        else:
            return dict()

    @classmethod
    def _meta_from_respose(cls, soup: bs4.BeautifulSoup):
        metadata = [{'Name': soup.find('h1').text.strip()}]
        try:
            items = soup.find('div', id='stn_metadata').find_all('p')[0].text
            items = items.split('\n\n')
            assert len(items) == 2
        except (AssertionError, TypeError, ValueError):
            return metadata
        metadata.extend(cls._parse_headers(items[0]))
        try:
            metadata.extend(cls._parse_attrs(items[1]))
        except (ValueError, TypeError):
            pass
        return metadata

    @classmethod
    def _parse_headers(cls, line_meta):
        station_headers = []
        headers = [i.strip() for i in line_meta.split('\n') if i]
        station_headers.append({'Statation Type': ', '.join(headers[0:-1])})
        station_headers.append({'Location': headers[-1]})
        return station_headers

    @classmethod
    def _parse_attrs(cls, line_attr: str) -> List[dict]:
        station_attrs = []
        attrs = [i for i in line_attr.split('\n') if i]
        for attr in attrs:
            k, v = attr.split(': ')
            station_attrs.append({k: v})
        return station_attrs
