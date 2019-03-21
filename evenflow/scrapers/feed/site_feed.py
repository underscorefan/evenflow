from typing import List, Dict, Union
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from dirtyfunc import Option, Either, Left, Right, Nothing

from evenflow import utreq
from evenflow.streams.messages.extracted_data_keeper import ExtractedDataKeeper

from .feed_scraper_state import FeedScraperState
from .feed_scraper import FeedResult, FeedScraper

URL = 'url'
PAGE = 'page'


class Selectors:
    def __init__(self, nextp: str, entries: str, links: str):
        self.next = nextp
        self.entries = entries
        self.links = links


class UrlContainer:
    def __init__(self, urls: List[str]):
        self.__urls = urls

    def to_dict(self, zip_with: List, repeat: bool = True) -> Dict:
        zip_with = zip_with * len(self) if repeat else zip_with
        return dict(zip(self.__urls, zip_with))

    def __len__(self):
        return len(self.__urls)

    @property
    def urls(self) -> List[str]:
        return self.__urls


class FeedContainer(UrlContainer):
    def __init__(self, urls: List[str], maybe_next: Option[str]):
        super().__init__(urls)
        self.maybe_next = maybe_next


class UrlExtractor:
    def __init__(self, page: BeautifulSoup):
        self.__page = page

    def make_url_container(self, list_selector: str) -> UrlContainer:
        return UrlContainer(self.__multiple_links(selector=list_selector))

    def make_feed_container(self, list_selector: str, next_page_selector: str) -> FeedContainer:
        return FeedContainer(
            urls=self.__multiple_links(list_selector),
            maybe_next=self.__get_link(next_page_selector)
        )

    def __multiple_links(self, selector: str) -> List[str]:
        return [a.get('href') for a in self.__page.select(selector)]

    def __get_link(self, selector: str) -> Option[str]:
        return Option(self.__page.select_one(selector)).map(lambda tag: tag.get('href'))


class SiteFeed(FeedScraper):
    def __init__(self, name: str, url: str, sel: Union[Dict, Selectors], stop_after: int, fake_news: bool):
        self.name = name
        self.url = url
        self.stop_after = stop_after
        self.sel = Selectors(**sel) if isinstance(sel, dict) else sel
        self.fake_news = fake_news

    def get_name(self) -> str:
        return self.name

    def recover_state(self, state: FeedScraperState) -> bool:
        if state.is_over:
            return False
        self.url = state.data[URL]
        self.stop_after = state.data[PAGE]
        return True

    async def fetch_links(self, session: ClientSession) -> Either[Exception, 'FeedResult']:
        maybe_feed = await self.__extract_feed(session)
        if maybe_feed.empty:
            return maybe_feed.on_left(lambda exc: Left(exc))

        feed: FeedContainer = maybe_feed.on_right()
        next_reader = feed.maybe_next.flat_map(self.__new_page)

        defined = FeedResult(
            articles=await self.__extract_links(session, *feed.urls),
            next_page=next_reader,
            state=self.to_state(over=next_reader.empty)
        )
        return Right(defined)

    def to_state(self, over: bool = False) -> FeedScraperState:
        return FeedScraperState(
            name=self.name,
            is_over=over,
            data={
                URL: self.url,
                PAGE: self.stop_after
            })

    def __new_page(self, new_page_url: str) -> Option['SiteFeed']:
        if self.stop_after == 0:
            return Nothing()

        defined = SiteFeed(
            name=self.name,
            url=new_page_url,
            sel=self.sel,
            stop_after=self.stop_after - 1,
            fake_news=self.fake_news
        )
        return Option(defined)

    async def __extract_links(self, session: ClientSession, *urls: str) -> ExtractedDataKeeper:
        arts = ExtractedDataKeeper()
        for url in urls:
            maybe_page = (await utreq.new_soup(url, session))\
                .map(lambda page: UrlExtractor(page).make_url_container(self.sel.links))\
                .map(lambda container: container.to_dict([(url, self.fake_news)]))
            arts.add_page_hrefs(url, maybe_page)
        return arts

    async def __extract_feed(self, session: ClientSession) -> Either[Exception, UrlContainer]:
        maybe_page = await utreq.new_soup(self.url, session)
        return maybe_page.map(
            lambda page: UrlExtractor(page).make_feed_container(
                list_selector=self.sel.entries,
                next_page_selector=self.sel.next
            )
        )
