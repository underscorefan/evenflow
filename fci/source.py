from aiohttp import ClientSession
from helpers.html import PageOps
from typing import List, Tuple, Optional, Dict
from helpers.func import mmap
import abc

ps = 'page_scraped'
pn = 'page_number'


class Source(abc.ABC):

    @abc.abstractmethod
    async def fetch_links(self, session: ClientSession) -> 'SpiderResult':
        pass


class BasicSourceData:
    def __init__(self, name: str, page_to_scrape: str, num_pages: int):
        self.name = name
        self.page_to_scrape = page_to_scrape
        self.num_pages = num_pages

    def to_dict(self) -> Dict:
        return {ps: self.page_to_scrape, pn: self.num_pages}


def bsd_from_dict(name, data: Dict) -> Optional[BasicSourceData]:
    try:
        return BasicSourceData(name=name, page_to_scrape=data[ps], num_pages=data[pn])
    except (KeyError, ValueError):
        return None


class SourceSpider(BasicSourceData):
    def __init__(
            self,
            name: str,
            page_to_scrape: str,
            next_page: str,
            articles: str,
            text_anchors: str,
            num_pages: int,
            is_fake: bool
    ):
        super().__init__(name, page_to_scrape, num_pages)
        self.next_page = next_page
        self.articles = articles
        self.text_anchors = text_anchors
        self.is_fake = is_fake

    def set_page_to_scrape(self, page_url: str):
        self.page_to_scrape = page_url

    def set_num_page(self, num_page: int):
        self.num_pages = num_page

    async def fetch_list(self, session: ClientSession) -> Tuple[List[str], str]:
        wr = PageOps(self.page_to_scrape, max_workers=2)
        k_art = "articles"
        k_n = "next"
        res = await wr.fetch_multiple_urls(self.articles, k_art)\
            .fetch_single_url(self.next_page, k_n)\
            .do_ops(session)
        return res[k_art], res[k_n]

    async def search_for_links(self, session: ClientSession) -> 'SpiderResult':
        feed_links, next_page = await self.fetch_list(session)
        links_from_articles = await self.__get_links_from(session, *feed_links)
        return SpiderResult(
            links=links_from_articles,
            next_spider=mmap(next_page, self.__new_page),
            old_spider=self
        )

    def __new_page(self, new_page_url: str) -> Optional['SourceSpider']:
        if self.num_pages == 0:
            return None
        return SourceSpider(
            name=self.name,
            page_to_scrape=new_page_url,
            next_page=self.next_page,
            articles=self.articles,
            text_anchors=self.text_anchors,
            num_pages=self.num_pages - 1,
            is_fake=self.is_fake
        )

    async def __get_links_from(self, session: ClientSession, *urls: str) -> Dict[str, Tuple[str, bool]]:
        k_out = "links"
        all_links: Dict[str, Tuple[str, bool]] = {}
        for url in urls:
            article_links = await PageOps(url, max_workers=1)\
                .fetch_multiple_urls(self.text_anchors, k_out)\
                .do_ops(session)
            all_links = {
                **all_links,
                **dict(zip(article_links[k_out], [(url, self.is_fake)] * len(article_links[k_out])))
            }
        return all_links

    def __str__(self):
        return f"home: {self.page_to_scrape}\tnext_page: {self.next_page}\t" \
            f"items: {self.articles}\tarticle_links: {self.text_anchors}\n"


class SpiderResult:
    def __init__(
            self,
            links: Dict[str, Tuple[str, bool]],
            next_spider: Optional[SourceSpider],
            old_spider: SourceSpider
    ):
        self.links = links
        self.next_spider = next_spider
        self.old_spider = old_spider
