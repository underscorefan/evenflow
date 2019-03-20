import abc
from dirtyfunc import Option
from aiohttp import ClientSession

from evenflow.messages.extracted_data_keeper import ExtractedDataKeeper
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


class FeedResult:
    def __init__(self, articles: ExtractedDataKeeper, next_reader: Option[FeedReader], current_reader_state: State):
        self.articles = articles
        self.next_reader = next_reader
        self.current_reader_state = current_reader_state
