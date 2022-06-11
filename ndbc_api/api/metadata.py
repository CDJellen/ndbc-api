from typing import TYPE_CHECKING, ClassVar, Iterator, Optional

if TYPE_CHECKING:
    from ..ndbc_api import NdbcApi

class Station:

    parent: ClassVar[NdbcApi]

    def __init__(
        self,
        station_id: str
    ):
        station_id: str