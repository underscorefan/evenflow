import abc
from aiohttp import ClientSession
from typing import List, Tuple, Optional, Dict, Union
from evenflow.helpers.func import mmap
from evenflow.helpers.html import PageOps
from .state import State


URL = 'url'
PAGE = 'page'


class Selectors:
    def __init__(self, nextp: str, entries: str, links: str):
        self.next = nextp
        self.entries = entries
        self.links = links


class Reader(abc.ABC):

    @abc.abstractmethod
    async def fetch_links(self, session: ClientSession) -> 'FeedResult':
        pass

    @abc.abstractmethod
    def recover_state(self, state: State):
        pass


class FeedReaderHTML(Reader):
    def __init__(self, name: str, url: str, sel: Union[Dict, Selectors], stop_after: int, fake_news: bool):
        self.name = name
        self.url = url
        self.stop_after = stop_after
        self.sel = Selectors(**sel) if isinstance(sel, dict) else sel
        self.fake_news = fake_news

    def set_page_to_scrape(self, page_url: str):
        self.url = page_url

    def set_stop_after(self, num_page: int):
        self.stop_after = num_page

    def recover_state(self, state: State):
        self.url = state.data[URL]
        self.stop_after = state.data[PAGE]

    async def fetch_list(self, session: ClientSession) -> Tuple[List[str], str]:
        wr = PageOps(self.url, max_workers=2)
        k_art = "articles"
        k_n = "next"
        res = await wr.fetch_multiple_urls(self.sel.entries, k_art)\
            .fetch_single_url(self.sel.next, k_n)\
            .do_ops(session)
        return res[k_art], res[k_n]

    async def fetch_links(self, session: ClientSession) -> 'FeedResult':
        feed_links, next_page = await self.fetch_list(session)
        links_from_articles = await self.__get_links_from(session, *feed_links)
        return FeedResult(
            links=links_from_articles,
            next_reader=mmap(next_page, self.__new_page),
            current_reader_state=self.__to_state(next_page)
        )

    def __to_state(self, next_page: Optional[str]) -> State:
        return State(
            name=self.name,
            is_over=next_page is not None,
            data={
                URL: self.url,
                PAGE: self.stop_after
            })

    def __new_page(self, new_page_url: str) -> Optional['FeedReaderHTML']:
        if self.stop_after == 0:
            return None

        return FeedReaderHTML(
            name=self.name,
            url=new_page_url,
            sel=self.sel,
            stop_after=self.stop_after - 1,
            fake_news=self.fake_news
        )

    async def __get_links_from(self, session: ClientSession, *urls: str) -> Dict[str, Tuple[str, bool]]:
        k_out = "links"
        all_links: Dict[str, Tuple[str, bool]] = {}
        for url in urls:
            article_links = await PageOps(url, max_workers=1)\
                .fetch_multiple_urls(self.sel.links, k_out)\
                .do_ops(session)
            all_links = {
                **all_links,
                **dict(zip(article_links[k_out], [(url, self.fake_news)] * len(article_links[k_out])))
            }
        return all_links


class FeedResult:
    def __init__(
            self,
            links: Dict[str, Tuple[str, bool]],
            next_reader: Optional[FeedReaderHTML],
            current_reader_state: State
    ):
        self.links = links
        self.next_reader = next_reader
        self.current_reader_state = current_reader_state
