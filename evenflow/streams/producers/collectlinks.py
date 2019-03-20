import asyncio

from typing import Dict, Tuple, List
from aiohttp import ClientSession
from dirtyfunc import Option
from evenflow.urlman import UrlSet
from evenflow.messages import Error, ExtractedDataKeeper
from evenflow.readers import FeedReader, State, FeedResult


class Sender:
    def __init__(self):
        self.__articles: ExtractedDataKeeper = ExtractedDataKeeper()
        self.__backup: Dict[str, Dict] = dict()
        self.__readers: List[FeedReader] = []

    def add_reader(self, reader: Option[FeedReader]):
        if not reader.empty:
            self.__readers.append(reader.on_value())

    def get_readers(self) -> List[FeedReader]:
        return self.__readers

    def merge_containers(self, articles: ExtractedDataKeeper):
        self.__articles = self.__articles + articles

    def add_to_backup(self, s: State):
        key, value = s.unpack()
        self.__backup[key] = value

    def get_links(self) -> Dict[str, Tuple[str, bool]]:
        return self.__articles.links_to_send

    @property
    def container(self) -> ExtractedDataKeeper:
        return self.__articles.set_backup(self.__backup)

    @property
    def errors(self) -> List[Error]:
        return self.__articles.errors

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
    def __init__(self, unrel: UrlSet, send_channel: asyncio.Queue):
        self.unrel = unrel
        self.send_channel = send_channel


async def collect_links(settings: LinkProducerSettings, to_read: List[FeedReader], session: ClientSession):
    iteration_manager = IterationManager(to_read)

    while iteration_manager.has_readers():
        readers = iteration_manager.get_readers()
        print([reader.get_name() for reader in readers])
        coroutines = [reader.fetch_links(session) for reader in readers]
        sender = Sender()

        for res in await asyncio.gather(*coroutines):
            if res.empty:
                print(res.on_left())
                continue
            feed_result: FeedResult = res.on_right()
            sender.add_reader(feed_result.next_reader)
            sender.merge_containers(feed_result.articles.filter(lambda k, _: settings.unrel.contains(k)))
            sender.add_to_backup(feed_result.current_reader_state)

        await settings.send_channel.put(sender.container)
        iteration_manager.set_readers(sender.get_readers())
