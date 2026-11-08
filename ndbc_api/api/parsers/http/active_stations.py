import xml.etree.ElementTree as ET
from typing import Any, List

from ndbc_api.api.parsers.http._xml import XMLParser
from ndbc_api.exceptions import ParserException


class ActiveStationsParser(XMLParser):
    """
    Parser for active station information from XML data.
    """

    @classmethod
    def parse_response(cls,
                       response: dict,
                       use_timestamp: bool = False) -> List[dict]:
        """
        Reads the response body and parses it into a list of dicts.

        Args:
            response (dict): The response dictionary containing the 'body' key.
            use_timestamp (bool): Flag to indicate if the timestamp should be used as an index (not applicable here).

        Returns:
            List[dict]: The parsed station information.
        """
        root = super(ActiveStationsParser, cls).root_from_response(response)
        try:
            station_data = []
            for station in root.findall('station'):
                station_info = {
                    'Station':
                        station.get('id'),
                    'Lat':
                        cls._safe_float(station.get('lat')),
                    'Lon':
                        cls._safe_float(station.get('lon')),
                    'Elevation':
                        cls._safe_float(station.get('elev')),
                    'Name':
                        station.get('name'),
                    'Owner':
                        station.get('owner'),
                    'Program':
                        station.get('pgm'),
                    'Type':
                        station.get('type'),
                    'Includes Meteorology':
                        station.get('met') == 'y',
                    'Includes Currents':
                        station.get('currents') == 'y',
                    'Includes Water Quality':
                        station.get('waterquality') == 'y',
                    'DART Program':
                        station.get('dart') == 'y'
                }
                station_data.append(station_info)

        except (ET.ParseError, ValueError, TypeError) as e:
            raise ParserException(f'Error parsing XML data: {e}') from e

        return station_data

    @classmethod
    def df_from_responses(cls,
                          response: dict,
                          use_timestamp: bool = False):
        """Parse response into a `pandas.DataFrame` (backward-compatible helper)."""
        from ndbc_api.api.parsers.http._base import pd
        if pd is None:
            raise ImportError(
                'Pandas is not installed. Please install it using `pip install pandas`.'
            )
        rows = cls.parse_response(response=response, use_timestamp=use_timestamp)
        return pd.DataFrame(rows)

    """ PRIVATE """

    @staticmethod
    def _safe_float(val: Any) -> float:
        if val is None or val == '':
            return float('nan')
        return float(val)
