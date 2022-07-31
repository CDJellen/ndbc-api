class CoreRequest:

    BASE_URL = 'https://www.ndbc.noaa.gov/'

    @classmethod
    def build_request(cls) -> str:
        return cls.BASE_URL
