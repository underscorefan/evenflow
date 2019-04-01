import asyncio

from typing import Dict, Tuple, List
from aiohttp import ClientSession
from dirtyfunc import Option
from evenflow.streams.messages import Error, DataKeeper, CollectorState
from evenflow.scrapers.feed import FeedScraper, FeedResult


class Sender:
    def __init__(self):
        self.__articles: DataKeeper = DataKeeper()
        self.__backup: Dict[str, CollectorState] = {}
        self.__feed_scrapers: List[FeedScraper] = []

    def add_feed_scraper(self, reader: Option[FeedScraper]):
        if not reader.empty:
            self.__feed_scrapers.append(reader.on_value())

    def get_feed_scrapers(self) -> List[FeedScraper]:
        return self.__feed_scrapers

    def merge_containers(self, articles: DataKeeper):
        self.__articles = self.__articles + articles

    def add_to_backup(self, s: CollectorState):
        self.__backup[s.name] = s

    def get_links(self) -> Dict[str, Tuple[str, bool]]:
        return self.__articles.links_to_send

    @property
    def container(self) -> DataKeeper:
        return self.__articles.append_states([v for v in self.__backup.values()])

    @property
    def errors(self) -> List[Error]:
        return self.__articles.errors

    def get_backup(self) -> Dict[str, CollectorState]:
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


async def collect_links_html(send_channel: asyncio.Queue, to_scrape: List[FeedScraper], session: ClientSession):
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
