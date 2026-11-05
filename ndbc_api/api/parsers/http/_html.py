from io import StringIO
from typing import List

from bs4 import BeautifulSoup

from ndbc_api.api.parsers.http._base import BaseParser

try:
    import pandas as pd
except ImportError:
    pd = None


class HtmlParser(BaseParser):

    @classmethod
    def dfs_from_responses(cls, responses: List[dict]) -> List['pd.DataFrame']:
        if pd is None:
            raise ImportError(
                'Pandas is not installed. Please install it using `pip install pandas`.'
            )
        components = []
        for response in responses:
            if response.get('status') == 200:
                soup = BeautifulSoup(response.get('body'), 'html.parser')
                tables = soup.find_all('table')
                components.extend(pd.read_html(StringIO(str(tables)), flavor='bs4'))
        return components
