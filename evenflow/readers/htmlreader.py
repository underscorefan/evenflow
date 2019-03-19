from aiohttp import ClientSession
from typing import List, Tuple, Dict, Union
from dirtyfunc import Option, Either, Left, Right, Nothing
from bs4 import BeautifulSoup

# from evenflow.helpers.html import PageOps
from evenflow.helpers.req import url_to_soup

from .feedreader import FeedResult, FeedReader, ArticlesContainer
from .state import State


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


class FeedReaderHTML(FeedReader):
    def __init__(self, name: str, url: str, sel: Union[Dict, Selectors], stop_after: int, fake_news: bool):
        self.name = name
        self.url = url
        self.stop_after = stop_after
        self.sel = Selectors(**sel) if isinstance(sel, dict) else sel
        self.fake_news = fake_news

    def get_name(self) -> str:
        return self.name

    def recover_state(self, state: State) -> bool:
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
        # next_reader = mmap(next_page, self.__new_page)

        defined = FeedResult(
            articles=await self.__extract_links(session, *feed.urls),
            next_reader=next_reader,
            current_reader_state=self.to_state(over=next_reader.empty)
        )
        return Right(defined)

    def to_state(self, over: bool = False) -> State:
        return State(
            name=self.name,
            is_over=over,
            data={
                URL: self.url,
                PAGE: self.stop_after
            })

    def __new_page(self, new_page_url: str) -> Option['FeedReaderHTML']:
        if self.stop_after == 0:
            return Nothing()

        defined = FeedReaderHTML(
            name=self.name,
            url=new_page_url,
            sel=self.sel,
            stop_after=self.stop_after - 1,
            fake_news=self.fake_news
        )
        return Option(defined)

    # async def __extract_feed_urls(self, session: ClientSession) -> Tuple[List[str], str]:
    #     wr = PageOps(self.url, max_workers=2)
    #     k_art = "articles"
    #     k_n = "next"
    #     res = await wr.fetch_multiple_urls(self.sel.entries, k_art)\
    #         .fetch_single_url(self.sel.next, k_n)\
    #         .do_ops(session)
    #     return res[k_art], res[k_n].on_value()
    #
    # async def __get_links_from(self, session: ClientSession, *urls: str) -> Dict[str, Tuple[str, bool]]:
    #     k_out = "links"
    #     all_links: Dict[str, Tuple[str, bool]] = {}
    #     for url in urls:
    #         article_links = await PageOps(url, max_workers=1)\
    #             .fetch_multiple_urls(self.sel.links, k_out)\
    #             .do_ops(session)
    #         all_links = {
    #             **all_links,
    #             **dict(zip(article_links[k_out], [(url, self.fake_news)] * len(article_links[k_out])))
    #         }
    #
    #     return all_links

    async def __extract_links(self, session: ClientSession, *urls: str) -> ArticlesContainer:
        arts = ArticlesContainer()
        for url in urls:
            maybe_page = (await url_to_soup(url, session))\
                .map(lambda page: UrlExtractor(page).make_url_container(self.sel.links))\
                .map(lambda container: container.to_dict([(url, self.fake_news)]))
            arts.add_page_hrefs(url, maybe_page)
        return arts

    async def __extract_feed(self, session: ClientSession) -> Either[Exception, UrlContainer]:
        maybe_page = await url_to_soup(self.url, session)
        return maybe_page.map(
            lambda page: UrlExtractor(page).make_feed_container(
                list_selector=self.sel.entries,
                next_page_selector=self.sel.next
            )
        )
