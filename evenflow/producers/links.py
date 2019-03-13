import asyncio

from typing import Dict, Tuple, List, Optional
from aiohttp import ClientSession

from evenflow.helpers.hostracker import HostTracker
from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.messages import LinkContainer
from evenflow.readers import FeedReader, State, FeedResult


class Sender:
    def __init__(self):
        self.__links: Dict[str, Tuple[str, bool]] = dict()
        self.__backup: Dict[str, Dict] = dict()
        self.__readers: List[FeedReader] = []

    def add_reader(self, reader: Optional[FeedReader]):
        if reader:
            self.__readers.append(reader)

    def get_readers(self) -> List[FeedReader]:
        return self.__readers

    def add_links(self, links: Dict[str, Tuple[str, bool]]):
        self.__links = {**self.__links, **links}

    def add_to_backup(self, s: State):
        key, value = s.unpack()
        self.__backup[key] = value

    def get_links(self) -> Dict[str, Tuple[str, bool]]:
        return self.__links

    def get_backup(self) -> Dict[str, Dict]:
        return self.__backup


class IterationManager:
    def __init__(self, initial_readers: List[FeedReader]):
        self.__readers = initial_readers

    def has_readers(self) -> bool:
        return len(self.__readers) > 0

    def set_readers(self, readers: List[FeedReader]):
        self.__readers = readers

    def get_readers(self) -> List[FeedReader]:
        return self.__readers


class LinkProducerSettings:
    def __init__(self, tracker: HostTracker, unrel: UnreliableSet, send_channel: asyncio.Queue):
        self.tracker = tracker
        self.unrel = unrel
        self.send_channel = send_channel


async def produce_links(settings: LinkProducerSettings, to_read: List[FeedReader], session: ClientSession):
    iteration_manager = IterationManager(to_read)
    while iteration_manager.has_readers():
        readers = iteration_manager.get_readers()
        print(readers)
        coroutines = [source.fetch_links(session) for source in readers]
        sender = Sender()

        for res in await asyncio.gather(*coroutines):
            feed_result: FeedResult = res
            sender.add_reader(feed_result.next_reader)
            sender.add_links({k: v for k, v in feed_result.links.items() if settings.unrel.contains(k)})
            sender.add_to_backup(feed_result.current_reader_state)

        await settings.send_channel.put(LinkContainer(links=sender.get_links(), backup=sender.get_backup()))
        iteration_manager.set_readers(sender.get_readers())
