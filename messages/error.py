from .storable import Storable
from typing import Optional, List


class Error(Storable):

    def __init__(self, msg: str, url: str, source: Optional[str] = None):
        self.msg = msg
        self.url = url
        self.source = source

    def to_sql_dict(self):
        return Error.__todict__(self.msg, self.url, self.source)

    @staticmethod
    def __todict__(m: str, u: str, s: str):
        return {
            "type": m,
            "url": u,
            "source_article": s
        }

    @staticmethod
    def columns() -> List[str]:
        return list(Error.__todict__('', '', '').keys())
