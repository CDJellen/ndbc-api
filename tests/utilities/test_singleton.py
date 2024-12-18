import pytest

from ndbc_api.utilities.singleton import Singleton


class MockSingleton(metaclass=Singleton):

    def __init__(self) -> None:
        self.mocked = True


@pytest.mark.private
def test_singleton_metaclass():
    _ = MockSingleton()
    assert hasattr(MockSingleton, '_instances')
    assert MockSingleton in MockSingleton._instances


@pytest.mark.private
def test_singleton_instance():
    mock1, mock2 = MockSingleton(), MockSingleton()
    assert id(mock1) == id(mock2)
