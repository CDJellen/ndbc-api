from ndbc_api.config import LOGGER_NAME


def test_config():
    assert isinstance(LOGGER_NAME, str)
