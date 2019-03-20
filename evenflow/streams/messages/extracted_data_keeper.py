from typing import Dict, Tuple, ItemsView, List, Callable

from dirtyfunc import Either

from evenflow.streams.messages import Error


class ExtractedDataKeeper:
    def __init__(self):
        self.__links_to_send: Dict[str, Tuple[str, bool]] = {}
        self.__errors: List[Error] = []
        self.__backup: Dict[str, Dict] = {}

    def add_page_hrefs(self, scraped_from: str, maybe_hrefs: Either[Exception, Dict[str, bool]]):
        if maybe_hrefs.empty:
            add_error = maybe_hrefs.on_left(lambda e: Error.from_exception(exc=e, url=scraped_from))
            self.__errors.append(add_error)
            return

        self.append_links(maybe_hrefs.on_right())

    def append_links(self, additional_links: Dict[str, Tuple[str, bool]]):
        self.__links_to_send = {**self.__links_to_send, **additional_links}

    def append_errors(self, additional_errors: List[Error]):
        self.__errors = self.__errors + additional_errors

    def filter(self, func: Callable[[str, Tuple[str, bool]], bool]) -> 'ExtractedDataKeeper':
        ret = ExtractedDataKeeper()
        ret.append_links({k: v for k, v in self.__links_to_send.items() if func(k, v)})
        ret.append_errors(self.errors)
        return ret

    def set_backup(self, bkp: Dict[str, Dict]) -> 'ExtractedDataKeeper':
        self.__backup = bkp
        return self

    def __add__(self, other: 'ExtractedDataKeeper') -> 'ExtractedDataKeeper':
        ret = ExtractedDataKeeper()
        ret.append_links(self.links_to_send)
        ret.append_links(other.links_to_send)
        ret.append_errors(other.errors + self.errors)
        ret.set_backup({**self.backup, **other.backup})
        return ret

    @property
    def items(self) -> ItemsView[str, Tuple[str, bool]]:
        return self.__links_to_send.items()

    @property
    def links_to_send(self) -> Dict[str, Tuple[str, bool]]:
        return self.__links_to_send

    @property
    def errors(self) -> List[Error]:
        return self.__errors

    @property
    def backup(self) -> Dict[str, Dict]:
        return self.__backup
