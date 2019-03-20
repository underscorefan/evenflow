import abc
import re
from aiohttp import ClientSession
from aiohttp.client_exceptions import InvalidURL
from newspaper.configuration import Configuration
from typing import Optional
from evenflow import utreq
from evenflow.streams.messages.article_extended import ArticleExtended
from evenflow.urlman import functions, UrlSet


class ArticleScraper(abc.ABC):

    @abc.abstractmethod
    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        pass


class DefaultArticleScraper(ArticleScraper):
    def __init__(self, article_link: str, source: str, fake: bool):
        self.article_link = article_link
        self.source = source
        self.fake = fake

    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        try:
            html = await utreq.get_html(self.article_link, session)
            # TODO quick fix, carry on either type
            if html.empty:
                return None

            a = ArticleExtended(
                url_to_visit=self.article_link,
                conf=conf,
                scraped_from=self.source,
                html=html.on_right(),
                fake=self.fake
            )

            a.correct_title()
            a.remove_newlines_from_fields()

            return a
        except InvalidURL:
            return None


class Archive(DefaultArticleScraper):
    def __init__(self, article_link: str, source: str, fake: bool, unreliable: UrlSet):
        super().__init__(article_link, source, fake)
        self.unrel = unreliable

    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        article = await super().get_data(session, conf)
        if article is None:
            return None

        try:
            url = article.soup.select_one("#HEADER > table input")["value"]
        except (KeyError, TypeError):
            return None

        if self.unrel.contains(url):
            article.set_actual_url(url)
            return article

        return None


class WebArchive(DefaultArticleScraper):
    def __init__(self, article_link: str, source: str, fake: bool, unreliable: UrlSet):
        super().__init__(article_link, source, fake)
        self.unrel = unreliable

    async def get_data(self, session: ClientSession, conf: Configuration) -> Optional[ArticleExtended]:
        maybe_url = re.findall("(https?://[^\\s]+)", functions.maintain_path(self.article_link))
        if len(maybe_url) > 0 and self.unrel.contains(maybe_url[0]):
            article = await super().get_data(session, conf)
            if article is None:
                return None
            article.set_actual_url(maybe_url[0])
            return article

        return None
