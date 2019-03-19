import abc
from typing import Dict, Tuple, List, Callable
from dirtyfunc import Option, Either
from aiohttp import ClientSession

from evenflow.messages import Error
from .state import State


class FeedReader(abc.ABC):

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def to_state(self, over: bool = False) -> State:
        pass

    @abc.abstractmethod
    async def fetch_links(self, session: ClientSession) -> 'FeedResult':
        pass

    @abc.abstractmethod
    def recover_state(self, state: State) -> bool:
        pass


class ArticlesContainer:
    def __init__(self):
        self.__links_to_send: Dict[str, Tuple[str, bool]] = {}
        self.__errors: List[Error] = []

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

    def filter(self, func: Callable[[str, Tuple[str, bool]], bool]) -> 'ArticlesContainer':
        ret = ArticlesContainer()
        ret.append_links({k: v for k, v in self.__links_to_send.items() if func(k, v)})
        ret.append_errors(self.errors)
        return ret

    def __add__(self, other: 'ArticlesContainer') -> 'ArticlesContainer':
        ret = ArticlesContainer()
        ret.append_links(self.links_to_send)
        ret.append_links(other.links_to_send)
        ret.append_errors(other.errors + self.errors)
        return ret

    @property
    def links_to_send(self) -> Dict[str, Tuple[str, bool]]:
        return self.__links_to_send

    @property
    def errors(self) -> List[Error]:
        return self.__errors


class FeedResult:
    def __init__(self, articles: ArticlesContainer, next_reader: Option[FeedReader], current_reader_state: State):
        self.articles = articles
        self.next_reader = next_reader
        self.current_reader_state = current_reader_state
