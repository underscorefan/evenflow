import asyncio

from typing import Dict, Tuple, List
from aiohttp import ClientSession
from dirtyfunc import Option
from evenflow.streams.messages import Error, ExtractedDataKeeper
from evenflow.scrapers.feed import FeedScraper, FeedScraperState, FeedResult


class Sender:
    def __init__(self):
        self.__articles: ExtractedDataKeeper = ExtractedDataKeeper()
        self.__backup: Dict[str, Dict] = dict()
        self.__feed_scrapers: List[FeedScraper] = []

    def add_feed_scraper(self, reader: Option[FeedScraper]):
        if not reader.empty:
            self.__feed_scrapers.append(reader.on_value())

    def get_feed_scrapers(self) -> List[FeedScraper]:
        return self.__feed_scrapers

    def merge_containers(self, articles: ExtractedDataKeeper):
        self.__articles = self.__articles + articles

    def add_to_backup(self, s: FeedScraperState):
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
    def __init__(self, feed_scrapers: List[FeedScraper]):
        self.__feed_scrapers = feed_scrapers

    def has_feed_scrapers(self) -> bool:
        return len(self.__feed_scrapers) > 0

    def set_feed_scrapers(self, readers: List[FeedScraper]):
        self.__feed_scrapers = readers

    def get_feed_scrapers(self) -> List[FeedScraper]:
        return self.__feed_scrapers


async def collect_links(send_channel: asyncio.Queue, to_scrape: List[FeedScraper], session: ClientSession):
    iteration_manager = IterationManager(to_scrape)

    while iteration_manager.has_feed_scrapers():
        feed_scrapers = iteration_manager.get_feed_scrapers()
        print([feed_scraper.get_name() for feed_scraper in feed_scrapers])
        coroutines = [feed_scraper.fetch_links(session) for feed_scraper in feed_scrapers]
        sender = Sender()

        for res in await asyncio.gather(*coroutines):
            if res.empty:
                print(res.on_left())
                continue
            feed_result: FeedResult = res.on_right()
            sender.add_feed_scraper(feed_result.next)
            sender.merge_containers(feed_result.articles)
            sender.add_to_backup(feed_result.state)

        await send_channel.put(sender.container)
        iteration_manager.set_feed_scrapers(sender.get_feed_scrapers())
