import abc
import re
from aiohttp import ClientSession
from aiohttp.client_exceptions import InvalidURL
from newspaper.configuration import Configuration
from functools import partial
from dirtyfunc import Either, Left
from evenflow import utreq
from evenflow.streams.messages.article_extended import ArticleExtended
from evenflow.urlman import functions


class ArticleScraper(abc.ABC):

    @abc.abstractmethod
    async def get_data(self, session: ClientSession, conf: Configuration) -> Either[Exception, ArticleExtended]:
        pass


class DefaultArticleScraper(ArticleScraper):
    def __init__(self, article_link: str, source: str, fake: bool):
        self.article_link = article_link
        self.partial = partial(ArticleExtended, url_to_visit=article_link, scraped_from=source, fake=fake)

    async def get_data(self, session: ClientSession, conf: Configuration) -> Either[Exception, ArticleExtended]:
        try:

            maybe_html = await utreq.get_html(self.article_link, session)
            maybe_article = maybe_html.map(lambda html: self.partial(conf=conf, html=html))
            corrected_title = maybe_article.map(lambda a: a.correct_title())
            return corrected_title.map(lambda a: a.remove_newlines_from_fields())

        except InvalidURL as e:
            return Left(e)


class Archive(DefaultArticleScraper):
    def __init__(self, article_link: str, source: str, fake: bool):
        super().__init__(article_link, source, fake)

    @staticmethod
    def title_extraction(article: ArticleExtended) -> ArticleExtended:
        url = article.soup.select_one("#HEADER > table input")["value"]
        article.set_actual_url(url)
        return article

    async def get_data(self, session: ClientSession, conf: Configuration) -> Either[Exception, ArticleExtended]:
        maybe_article = await super().get_data(session, conf)
        return maybe_article.flat_map(lambda a: Either.attempt(partial(self.title_extraction, a)))


class WebArchive(DefaultArticleScraper):
    def __init__(self, article_link: str, source: str, fake: bool):
        super().__init__(article_link, source, fake)

    async def get_data(self, session: ClientSession, conf: Configuration) -> Either[Exception, ArticleExtended]:
        maybe_url = re.findall("(https?://[^\\s]+)", functions.maintain_path(self.article_link))

        if len(maybe_url) > 0:
            return (await super().get_data(session, conf)).map(lambda a: a.set_actual_url(maybe_url[0]))

        return Left(ValueError(f"encoded URL not found in {self.article_link}"))
