from collections import defaultdict
from calendar import month_abbr
from datetime import datetime
from typing import List, Tuple

import bs4


class StationParser:

    BASE_URL = 'https://www.ndbc.noaa.gov'

    @classmethod
    def _parse_li_urls(
        cls, urls: List[bs4.element.Tag]
    ) -> List[Tuple[str, str]]:
        parsed = []
        current_year = datetime.now().year
        for raw_url in urls:
            name = raw_url.text.strip()
            name = f'{name} {current_year}' if name in month_abbr else name
            url = f'{cls.BASE_URL}{raw_url.get("href")}'
            parsed.append((name, url))
        return parsed

    @classmethod
    def _build_available_measurements(
        cls, line_items: List[bs4.element.Tag]
    ) -> dict:
        # unpack nested lists
        nested = [li for li in line_items for li in li.find_all('li')]
        nested = [
            li
            for li in nested
            if li.get('href') is not None and 'plot' not in li.get('href')
        ]
        line_items = [li for li in line_items if len(li.find_all('li')) == 0]
        line_items.extend(nested)
        available_measurements = defaultdict(dict)
        for li in line_items:
            if 'Search' in li.text:
                break  # end of available measurements
            new_measurement = cls._parse_list_item(li)
            if new_measurement:
                k = list(new_measurement.keys())[0]  # guaranteed one key
            else:
                continue
            available_measurements[k].update(new_measurement[k])
        return dict(available_measurements)
