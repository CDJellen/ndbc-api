import xml.etree.ElementTree as ET
import pandas as pd

from ndbc_api.exceptions import ParserException
from ndbc_api.api.parsers.http._xml import XMLParser


class HistoricalStationsParser(XMLParser):
    """
    Parser for active station information from XML data.
    """
    
    @classmethod
    def df_from_response(cls, response: dict, use_timestamp: bool = False) -> pd.DataFrame:
        """
        Reads the response body and parses it into a DataFrame.

        Args:
            response (dict): The response dictionary containing the 'body' key.
            use_timestamp (bool): Flag to indicate if the timestamp should be used as an index (not applicable here).

        Returns:
            pd.DataFrame: The parsed DataFrame containing station information.
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
                        'Station': station_id,
                        'Lat': float(history.get('lat')),
                        'Lon': float(history.get('lng')),
                        'Elevation': float(history.get('elev')) if history.get('elev') else pd.NA,
                        'Name': station_name,
                        'Owner': station_owner,
                        'Program': station_program,
                        'Type': station_type,
                        'Includes Meteorology': history.get('met') == 'y',
                        'Hull Type': history.get('hull'),
                        'Anemometer Height': float(history.get('anemom_height')) if history.get('anemom_height') else pd.NA,
                        'Start Date': history.get('start'),
                        'End Date': history.get('stop'),
                    }
                    station_data.append(station_info)

            df = pd.DataFrame(station_data)

        except ET.ParseError as e:
            raise ParserException(f"Error parsing XML data: {e}") from e

        return df