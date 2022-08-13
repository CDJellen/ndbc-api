from typing import List

import pandas as pd

from ndbc_api.api.parsers._html import HtmlParser


class StationsParser(HtmlParser):

    DATA_STATUS = {
        'S': 'Sensor/system failure.',
        'E': 'Data under evaluation, not reported.',
        'N': 'No sensor installed.',
        'Y': 'Parameter reported, no percentage',
        'D': 'Station disestablished.',
        'R': 'Buoy recovered',
    }
    APPLY_MAP = (
        lambda x: StationsParser.DATA_STATUS[x]
        if str(x) in StationsParser.DATA_STATUS
        else x
    )

    @classmethod
    def df_from_responses(cls, responses: List[dict]) -> pd.DataFrame:
        dfs = super(StationsParser, cls).dfs_from_responses(responses)
        station_dfs = []
        for df in dfs:
            if 'Station' in df.columns:
                station_dfs.append(df)

        for df in station_dfs:
            df.Station = df.Station.apply(lambda x: x.strip('*'))

        df = pd.concat(station_dfs)
        df = df.applymap(cls.APPLY_MAP)
        return df
