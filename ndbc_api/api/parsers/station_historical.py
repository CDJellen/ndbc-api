import re

import bs4

from ndbc_api.api.parsers._station import StationParser


class HistoricalParser(StationParser):

    LIST_IDENTIFIER = re.compile(
        'Available historical data for station .{5} include:'
    )

    @classmethod
    def available_measurements(cls, response: dict) -> dict:
        if response.get('status') == 200:
            soup = bs4.BeautifulSoup(response.get('body'), 'html.parser')
            p_tag = soup.find('p', text=cls.LIST_IDENTIFIER)
            line_items = p_tag.find_next_siblings('ul')[0].find_all('li')
            return cls._build_available_measurements(line_items=line_items)
        else:
            return dict()

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
