import abc
from aiohttp import ClientSession
from typing import List, Tuple, Optional, Dict
from evenflow.helpers.func import mmap
from evenflow.helpers.html import PageOps


_ps = 'page_scraped'
_pn = 'page_number'


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
    def update_from_backup(self, bkp: Dict):
        pass


class FeedReader:

    def __init__(self, name: str, url: str, stop_after: int):
        self.name = name
        self.url = url
        self.stop_after = stop_after

    def to_dict(self) -> Dict:
        return {_ps: self.url, _pn: self.stop_after}

    @staticmethod
    def from_dict(name: str, data: Dict) -> Optional['FeedReader']:
        try:
            return FeedReader(name=name, url=data[_ps], stop_after=data[_pn])
        except (KeyError, ValueError):
            return None


class FeedReaderHTML(FeedReader):
    def __init__(self, name: str, url: str, sel: Selectors, stop_after: int, fake_news: bool):
        super().__init__(name, url, stop_after)
        self.sel = Selectors(**sel) if isinstance(sel, dict) else sel
        self.fake_news = fake_news

    def set_page_to_scrape(self, page_url: str):
        self.url = page_url

    def set_stop_after(self, num_page: int):
        self.stop_after = num_page

    async def fetch_list(self, session: ClientSession) -> Tuple[List[str], str]:
        wr = PageOps(self.url, max_workers=2)
        k_art = "articles"
        k_n = "next"
        res = await wr.fetch_multiple_urls(self.sel.entries, k_art)\
            .fetch_single_url(self.sel.next, k_n)\
            .do_ops(session)
        return res[k_art], res[k_n]

    async def search_for_links(self, session: ClientSession) -> Optional['FeedResult']:
        feed_links, next_page = await self.fetch_list(session)

        links_from_articles = await self.__get_links_from(session, *feed_links)
        return FeedResult(
            links=links_from_articles,
            next_reader=mmap(next_page, self.__new_page),
            current_reader=self
        )

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
            current_reader: FeedReaderHTML
    ):
        self.links = links
        self.next_reader = next_reader
        self.current_reader = current_reader
