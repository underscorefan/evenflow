import abc
from typing import Dict, List


class Storable(abc.ABC):

    @abc.abstractmethod
    def to_sql_dict(self) -> Dict:
        pass

    @staticmethod
    @abc.abstractmethod
    def columns() -> List[str]:
        pass
