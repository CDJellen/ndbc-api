import logging
from typing import Union, List
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from utilities.req_handler import RequestHandler
from config import LOGGER_NAME


DEFAULT_CACHE_LIMIT = 36


class NdbcApi:

    _is_context_manager = False
    log = logging.getLogger(LOGGER_NAME)

    def __init__(self, logging_level: int = logging.WARNING, cache_limit: int = DEFAULT_CACHE_LIMIT, *args, **kwargs):
        self.log.setLevel(logging_level)
        self.cache_limit = cache_limit
        self.handler = RequestHandler(cache_limit=self.cache_limit)

    def update_cache_limit(self, new_limit: int) -> None:
        self.handler.update_cache_limit(cache_limit=new_limit)

    def get_station_data(self, mode: str = 'stdmet', station_id: Union[int, str, None] = None, start_time: Union[str, datetime] = datetime.now()-timedelta(days=30), end_time: Union[str, datetime] = datetime.now(), cols: Union[List[str], None] = None, lat: Union[str, float, None]  = None, lon: Union[str, float, None] = None) -> pd.DataFrame:
        mode = mode.lower()
        if not isinstance(start_time, datetime):
            try:
                start_time = datetime.fromisoformat(start_time)
            except ValueError as e:
                raise ValueError('Please supply start time as ISO formatted string.') from e
        if not isinstance(end_time, datetime):
            try:
                end_time = datetime.fromisoformat(end_time)
            except ValueError as e:
                raise ValueError('Please supply start time as ISO formatted string.') from e
        if mode not in ['cwind', 'stdmet']:
            raise NotImplementedError('Please specify `cwind` for continuous winds or `stdmet` for standard meterological data.')
        if station_id is None and (lat is None or lon is None):
            raise ValueError('Please specify an NDBC station ID or provide lat-lon information.')
        if not station_id and (lat and lon):
            station_id = self.get_station_id_from_lat_lon(lat=lat, lon=lon)
        if mode == 'stdmet':
            return self.get_stdmet(station_id=station_id, start_time=start_time, end_time=end_time, cols=cols)
        else:
            return self.get_cwind(station_id=station_id, start_time=start_time, end_time=end_time, cols=cols)

    def get_stdmet(self, station_id: int, start_time: datetime, end_time: datetime, cols: List[str]):
        responses = self.handler.get_station_data(station_id=station_id, start_time=start_time, end_time=end_time, mode='stdmet')
        df = self.df_from_responses(mode='stdmet', cols=cols, responses=responses)
        df = df.loc[start_time:end_time]
        return df.reset_index(drop=True)

    def get_cwind(self, station_id: int, start_time: datetime, end_time: datetime, cols: List[str]):
        responses = self.handler.get_station_data(station_id=station_id, start_time=start_time, end_time=end_time, mode='cwind')
        df = self.df_from_responses(mode='cwind', cols=cols, responses=responses)
        df = df.loc[start_time:end_time]
        return df.reset_index(drop=True)

    def df_from_responses(self, mode: str, responses: List[dict], cols: List[str] = None) -> pd.DataFrame:
        if mode == 'stdmet':
            df = pd.DataFrame()
            for response in responses:
                tmp = self.df_from_stdmet_response(response=response)
                df = df.append(tmp)
        elif mode == 'cwind':
            df = pd.DataFrame()
            for response in responses:
                tmp = self.df_from_cwind_response(response=response)
                df = df.append(tmp)
        else:
            raise NotImplementedError
        if cols:
            to_drop = set(df.columns) - set(cols)
            df.drop(columns=list(to_drop), inplace=True)
        df = df.set_index(df.timestamp).sort_index()
        return df

    @staticmethod
    def df_from_stdmet_response(response: dict) -> pd.DataFrame:
        if response.get('status') != 200:
            return pd.DataFrame()
        rows = [row.split(' ') for row in response.get('body').split('\n')[2:]]
        row_tuples = []

        for row in rows:
            row_values = [y for y in row if y != '']
            if len(row_values) == 19:
                row_values = row_values[0:17]+row_values[-1:]
            elif len(row_values) > 19:
                return pd.DataFrame()
            row_tuples.append(tuple(row_values))

        df = pd.DataFrame.from_records(row_tuples, columns=['year', 'month', 'day', 'hour', 'minute', 'wind_direction', 'wind_speed', 'wind_speed_gust', 'wave_height', 'dpd', 'apd', 'mwd', 'pressure', 'temperature_air', 'temperature_water', 'dew_point_temperature', 'visibility', 'tide'])
        df.dropna(inplace=True)
        df.insert(0, 'timestamp', pd.to_datetime(df['day'].astype(str) \
                                    + '-' + df['month'].astype(str) \
                                    + '-' + df['year'].astype(str) \
                                    + ' ' + df['hour'].astype(str) \
                                    + ':' + df['minute'].astype(str),
                                    format='%d-%m-%Y %H:%M'))

        df = df.replace('MM', np.nan)  # missing measurement
        df.wind_direction = pd.to_numeric(df.wind_direction)
        df.wind_speed = pd.to_numeric(df.wind_speed)
        df.wind_speed_gust = pd.to_numeric(df.wind_speed_gust)
        df.wave_height = pd.to_numeric(df.wave_height)
        df.dpd = pd.to_numeric(df.dpd)
        df.apd = pd.to_numeric(df.apd)
        df.mwd = pd.to_numeric(df.mwd)
        df.pressure = pd.to_numeric(df.pressure)
        df.temperature_air = pd.to_numeric(df.temperature_air)
        df.temperature_water = pd.to_numeric(df.temperature_water)
        df.dew_point_temperature = pd.to_numeric(df.dew_point_temperature)
        df.visibility = pd.to_numeric(df.visibility)
        df.tide = pd.to_numeric(df.tide)
        df = df.replace(999, np.nan)
        df = df.replace(99.0, np.nan)

        return df

    @staticmethod
    def df_from_cwind_response(response: dict) -> pd.DataFrame:
        if response.get('status') != 200:
            return pd.DataFrame()
        rows = [row.split(' ') for row in response.get('body').split('\n')[2:]]
        row_tuples = []

        for row in rows:
            row_values = [y for y in row if y != '']
            if len(row_values) >= 11:
                row_values = row_values[0:10]
            row_tuples.append(tuple(row_values))

        df = pd.DataFrame.from_records(row_tuples,
                                       columns=['year',
                                                'month',
                                                'day',
                                                'hour',
                                                'minute',
                                                'wind_direction',
                                                'wind_speed',
                                                'gdr',
                                                'wind_speed_gust',
                                                'gtime'
                                            ])
        df.dropna(inplace=True)
        df.insert(0, 'timestamp', pd.to_datetime(df['day'].astype(str) \
                                    + '-' + df['month'].astype(str) \
                                    + '-' + df['year'].astype(str) \
                                    + ' ' + df['hour'].astype(str) \
                                    + ':' + df['minute'].astype(str),
                                    format='%d-%m-%Y %H:%M'))
        df = df.replace('MM', np.nan)  # missing measurement
        df.wind_direction = pd.to_numeric(df.wind_direction)
        df.wind_speed = pd.to_numeric(df.wind_speed)
        df.wind_speed_gust = pd.to_numeric(df.wind_speed_gust)
        df.gtime = pd.to_numeric(df.gtime)
        df = df.replace(9999, np.nan)
        df = df.replace(999, np.nan)
        df = df.replace(99.0, np.nan)

        return df

    def get_station_id_from_lat_lon(self, lat: str, lon: str) -> str:
        return 'tplm2'

if __name__ == '__main__':
    api = NdbcApi()
    vals_cwind = api.get_station_data(
        mode='cwind',
        start_time=datetime.fromisoformat('2020-01-01'),
        end_time=datetime.fromisoformat('2022-05-20'),
        station_id='tplm2',
        cols=None,
        lat=None,
        lon=None
    )
    print(len(vals_cwind))
    vals = api.get_station_data(
        mode='stdmet',
        start_time=datetime.fromisoformat('2020-01-01'),
        end_time=datetime.fromisoformat('2022-05-20'),
        station_id='tplm2',
        cols=None,
        lat=None,
        lon=None
    )
    print(len(vals))
