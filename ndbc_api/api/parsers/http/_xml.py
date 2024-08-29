import re
import xml.etree.ElementTree as ET

from ndbc_api.api.parsers.http._base import BaseParser
from ndbc_api.exceptions import ParserException


class XMLParser(BaseParser):
    """
    Parser for XML data.
    """

    @classmethod
    def root_from_response(cls, response: dict) -> ET.ElementTree:
        """Parse the response body (string-valued XML) to ET
        
        Args:
            response (dict): The successful HTTP response
        """

        body = response.get('body')

        try:
            root = ET.fromstring(body)
            return ET.ElementTree(root) 
        except Exception as e:
            raise ParserException("failed to obtain XML root from response body") from e
