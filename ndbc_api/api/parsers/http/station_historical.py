import re

import bs4

from ndbc_api.api.parsers.http._station import StationParser


class HistoricalParser(StationParser):

    LIST_IDENTIFIER = re.compile(
        r'Available historical data for station\s+\S+\s+include:',
        re.IGNORECASE,
    )

    @classmethod
    def available_measurements(cls, response: dict) -> dict:
        if response.get('status') == 200:
            soup = bs4.BeautifulSoup(response.get('body') or '', 'html.parser')
            p_tag = soup.find('p', string=cls.LIST_IDENTIFIER)
            if not p_tag:
                return dict()
            uls = p_tag.find_next_siblings('ul')
            if not uls:
                return dict()
            line_items = uls[0].find_all('li')
            return cls._build_available_measurements(line_items=line_items)
        else:
            return dict()

    """ PRIVATE """

    @classmethod
    def _parse_list_item(cls, li: bs4.element.Tag) -> dict:
        measurement_item = dict()
        try:
            title = li.find('b').text.strip(': ')
            parsed = cls._parse_li_urls(li.find_all('a'))
        except AttributeError:
            return measurement_item
        measurement_item[title] = dict()
        for name, url in parsed:
            measurement_item[title][name] = url
        return measurement_item
