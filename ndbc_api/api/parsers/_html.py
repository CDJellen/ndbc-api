from typing import List

import pandas as pd
from bs4 import BeautifulSoup

from ndbc_api.api.parsers._base import BaseParser


class HtmlParser(BaseParser):

    INDEX_COL = None

    @classmethod
    def dfs_from_responses(cls, responses: List[dict]) -> List[pd.DataFrame]:
        components = []
        for response in responses:
            if response.get('status') == 200:
                soup = BeautifulSoup(response.get('body'), 'html.parser')
                tables = soup.find_all('table')
                components.extend(pd.read_html(str(tables), flavor='bs4'))
        return components
