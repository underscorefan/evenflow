import abc
import re
from aiohttp import ClientSession
from aiohttp.client_exceptions import InvalidURL
from messages.article_ext import ArticleExtended
from helpers.req import fetch_html_get, urlman
from newspaper.configuration import Configuration
from helpers.unreliableset import UnreliableSet
from typing import Optional


class Extractor(abc.ABC):

    @abc.abstractmethod
    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        pass


class ArticleExtractor(Extractor):
    def __init__(self, article_link: str, source: str, fake: bool):
        self.article_link = article_link
        self.source = source
        self.fake = fake

    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        try:
            html = await fetch_html_get(self.article_link, session)
            a = ArticleExtended(
                url_to_visit=self.article_link,
                conf=conf,
                scraped_from=self.source,
                html=html,
                fake=self.fake
            )

            a.correct_title()
            a.remove_newlines_from_fields()

            return a
        except InvalidURL:
            return None


class Archive(ArticleExtractor):
    def __init__(self, article_link: str, source: str, fake: bool, unreliable: UnreliableSet):
        super().__init__(article_link, source, fake)
        self.unrel = unreliable

    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        article = await super().get_data(session, conf)
        if article is None:
            return None

        try:
            url = article.soup.select_one("#HEADER > table input")["value"]
        except KeyError:
            return None

        if self.unrel.contains(url):
            article.set_actual_url(url)
            return article

        return None


class WebArchive(ArticleExtractor):
    def __init__(self, article_link: str, source: str, fake: bool, unreliable: UnreliableSet):
        super().__init__(article_link, source, fake)
        self.unrel = unreliable

    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        maybe_url = re.findall("(https?://[^\\s]+)", urlman.maintain_path(self.article_link))
        if len(maybe_url) > 0 and self.unrel.contains(maybe_url[0]):
            article = await super().get_data(session, conf)
            if article is None:
                return None
            article.set_actual_url(maybe_url[0])
            return article

        return None


def scraper_factory(link: str, source: str, fake: bool, unreliable: UnreliableSet) -> Extractor:
    if re.match('^https?://archive[.]', link):
        return Archive(link, source, fake, unreliable)

    if re.match('^https?://web[.]archive[.]', link):
        return WebArchive(link, source, fake, unreliable)

    return ArticleExtractor(link, source, fake)
