from .storable import Storable
from typing import Optional, List
from datetime import datetime


class Error(Storable):

    def __init__(self, msg: str, url: str, fake: bool, info: Optional[str] = None, source: Optional[str] = None):
        self.msg = msg
        self.url = url
        self.fake = fake
        self.info = info
        self.source = source
        self.timestamp = datetime.now()

    def to_sql_dict(self):
        return Error.__todict__(
            self.msg,
            self.url,
            self.source,
            self.fake,
            self.info,
            self.timestamp
        )

    @staticmethod
    def from_exception(exc: Exception, url: str, fake: bool, source: Optional[str] = None) -> 'Error':
        return Error(msg=type(exc).__name__, url=url, source=source, info=str(exc), fake=fake)

    @staticmethod
    def __todict__(m: str, u: str, s: Optional[str], f: bool, i: Optional[str], t: datetime):
        return {
            "type": m,
            "url": u,
            "source_article": s,
            "from_fake": f,
            "additional_info": i,
            "creation_date": t
        }

    @staticmethod
    def columns() -> List[str]:
        return list(Error.__todict__('', '', '', False, '', datetime.now()).keys())
