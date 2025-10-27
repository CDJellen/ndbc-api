import bs4

from ndbc_api.api.parsers.http._station import StationParser


class RealtimeParser(StationParser):

    @classmethod
    def available_measurements(cls, response: dict) -> dict:
        if response.get('status') != 200:
            return dict()
        
        soup = bs4.BeautifulSoup(response.get('body'), 'html.parser')
        items = soup.find('section', {"class": "data"})
        
        if items:
            # Find all <a> tags whose href attribute starts with "/data/realtime2/"
            links = items.select('a[href^="/data/realtime2/"]')
            # Get the parent <li> tag for each link
            line_items = [link.find_parent('li') for link in links if link.find_parent('li')]
            # Remove duplicates while preserving order
            unique_line_items = list(dict.fromkeys(line_items))
            
            return cls._build_available_measurements(line_items=unique_line_items)
        else:
            return dict()

    @classmethod
    def _parse_list_item(cls, li: bs4.element.Tag) -> dict:
        measurement_item = dict()
        try:
            title = li.text.split('\n')[0]
            parsed = cls._parse_li_urls(li.find_all('a'))
        except AttributeError:
            return measurement_item
        measurement_item[title] = dict()
        for name, url in parsed:
            measurement_item[title][name] = url
        return measurement_item
