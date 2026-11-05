import xml.etree.ElementTree as ET
from typing import Any, List

from ndbc_api.api.parsers.http._xml import XMLParser
from ndbc_api.exceptions import ParserException


class HistoricalStationsParser(XMLParser):
    """
    Parser for historical station information from XML data.
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
        root = super(HistoricalStationsParser, cls).root_from_response(response)
        try:
            station_data = []
            for station in root.findall('station'):
                station_id = station.get('id')
                station_name = station.get('name')
                station_owner = station.get('owner')
                station_program = station.get('pgm')
                station_type = station.get('type')

                for history in station.findall('history'):
                    station_info = {
                        'Station':
                            station_id,
                        'Lat':
                            cls._safe_float(history.get('lat')),
                        'Lon':
                            cls._safe_float(history.get('lng')),
                        'Elevation':
                            cls._safe_float(history.get('elev')),
                        'Name':
                            station_name,
                        'Owner':
                            station_owner,
                        'Program':
                            station_program,
                        'Type':
                            station_type,
                        'Includes Meteorology':
                            history.get('met') == 'y',
                        'Hull Type':
                            history.get('hull'),
                        'Anemometer Height':
                            cls._safe_float(history.get('anemom_height')),
                        'Start Date':
                            history.get('start'),
                        'End Date':
                            history.get('stop'),
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
