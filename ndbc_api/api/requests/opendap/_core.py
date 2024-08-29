class CoreRequest:

    BASE_URL = 'https://dods.ndbc.noaa.gov/thredds/'

    @classmethod
    def build_request(cls) -> str:
        return cls.BASE_URL
