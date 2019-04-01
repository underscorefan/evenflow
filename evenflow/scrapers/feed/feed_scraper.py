import abc
from dirtyfunc import Option
from aiohttp import ClientSession

from evenflow.streams.messages.data_keeper import DataKeeper
from evenflow.streams.messages.collector_state import CollectorState


class FeedScraper(abc.ABC):

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def to_state(self, over: bool = False) -> CollectorState:
        pass

    @abc.abstractmethod
    async def fetch_links(self, session: ClientSession) -> 'FeedResult':
        pass

    @abc.abstractmethod
    def recover_state(self, state: CollectorState) -> bool:
        pass


class FeedResult:
    def __init__(self, articles: DataKeeper, next_page: Option[FeedScraper], state: CollectorState):
        self.articles = articles
        self.next = next_page
        self.state = state
