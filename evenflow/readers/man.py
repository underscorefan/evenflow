import asyncio

from typing import List, ItemsView, Tuple, Dict, Optional
from aiohttp import ClientSession
# from collections import defaultdict

from evenflow.helpers.func import mmap
from evenflow.helpers.hostracker import HostTracker
from evenflow.helpers.unreliableset import UnreliableSet
# from evenflow.helpers.req import maintain_netloc
from evenflow.messages import LinkContainer

from .htmlreader import FeedReaderHTML
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
    def __init__(self, tracker: HostTracker, unrel: UnreliableSet, send_channel: asyncio.Queue, session: ClientSession):
        self.tracker = tracker
        self.unrel = unrel
        self.send_channel = send_channel
        self.session = session


async def produce_links(settings: LinkProducerSettings, to_read: List[FeedReader]):
    iteration_manager = IterationManager(to_read)
    while iteration_manager.has_readers():
        coroutines = [source.fetch_links(settings.session) for source in iteration_manager.get_readers()]
        sender = Sender()

        for res in await asyncio.gather(*coroutines):
            feed_result: FeedResult = res
            sender.add_reader(feed_result.next_reader)
            sender.add_links({k: v for k, v in feed_result.links.items() if settings.unrel.contains(k)})
            sender.add_to_backup(feed_result.current_reader_state)

        await settings.send_channel.put(LinkContainer(links=sender.get_links(), backup=sender.get_backup()))
        iteration_manager.set_readers(sender.get_readers())


class SourceManager:
    def __init__(self, sources: List[FeedReader], tracker: HostTracker, unrel: UnreliableSet):
        self.original_sources = sources
        self.tracker = tracker
        self.unrel = unrel

    async def fetch_articles(self, session: ClientSession, q: asyncio.Queue):
        sources = self.original_sources
        while len(sources) > 0:
            coroutines = [source.fetch_links(session) for source in sources]

            all_links = dict()
            backup = dict()
            sources: List[FeedReaderHTML] = []

            for result in await asyncio.gather(*coroutines):
                feed_result: FeedResult = result
                mmap(feed_result.next_reader, sources.append)
                all_links = {
                    **all_links,
                    **({k: v for k, v in feed_result.links.items() if self.unrel.contains(k)})
                }

                key, value = feed_result.current_reader_state.unpack()
                backup[key] = value

            await q.put(LinkContainer(links=all_links, backup=backup))

    # async def __group_by_host(self, links: Dict[str, str]) -> Tuple[ItemsView[str, set], set]:
    #     d = defaultdict(set)
    #     errors = set()
    #     for link, from_article in links.items():
    #         netloc = maintain_netloc(link)
    #         if not self.unrel.contains(url=netloc, netloc=False):
    #             continue
    #         k = await self.tracker.get_ip(url=netloc, parse=False)
    #         if k is not None:
    #             d[k].add((link, from_article))
    #         else:
    #             errors.add((link, from_article))
    #     return d.items(), errors
    #
    # def store_tracker(self, path: str):
    #     self.tracker.store_dict(path=path)
