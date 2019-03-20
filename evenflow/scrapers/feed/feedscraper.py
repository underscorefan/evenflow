import abc
from dirtyfunc import Option
from aiohttp import ClientSession

from evenflow.streams.messages.extracted_data_keeper import ExtractedDataKeeper
from .feedscraperstate import FeedScraperState


class FeedScraper(abc.ABC):

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def to_state(self, over: bool = False) -> FeedScraperState:
        pass

    @abc.abstractmethod
    async def fetch_links(self, session: ClientSession) -> 'FeedResult':
        pass

    @abc.abstractmethod
    def recover_state(self, state: FeedScraperState) -> bool:
        pass


class FeedResult:
    def __init__(self, articles: ExtractedDataKeeper, next_page: Option[FeedScraper], state: FeedScraperState):
        self.articles = articles
        self.next = next_page
        self.state = state
