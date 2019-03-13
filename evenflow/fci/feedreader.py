import abc
from typing import Dict, Tuple, Optional
from aiohttp import ClientSession
from evenflow.fci import State


class FeedReader(abc.ABC):

    @abc.abstractmethod
    def get_name(self) -> str:
        pass

    @abc.abstractmethod
    def to_state(self, over: bool = False):
        pass

    @abc.abstractmethod
    async def fetch_links(self, session: ClientSession) -> 'FeedResult':
        pass

    @abc.abstractmethod
    def recover_state(self, state: State) -> bool:
        pass


class FeedResult:
    def __init__(
            self,
            links: Dict[str, Tuple[str, bool]],
            next_reader: Optional[FeedReader],
            current_reader_state: State
    ):
        self.links = links
        self.next_reader = next_reader
        self.current_reader_state = current_reader_state
