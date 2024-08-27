import re
import xml.etree.ElementTree as ET

from ndbc_api.api.parsers._base import BaseParser
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
        cleaned_body = body.replace('\n', '').strip() 
        cleaned_body = cleaned_body.replace('<?xml version="1.0" encoding="UTF-8"?>', '')
        cleaned_body = re.sub('', '', cleaned_body) 
        try:
            root = ET.fromstring(cleaned_body)
            return ET.ElementTree(root) 
        except Exception as e:
            raise ParserException("failed to obtain XML root from response body") from e

        return root