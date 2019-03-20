import re

from evenflow.urlman import UrlSet
from .articlescraper import ArticleScraper, Archive, WebArchive, DefaultArticleScraper


def article_factory(link: str, source: str, fake: bool, unreliable: UrlSet) -> ArticleScraper:
    if re.match('^https?://archive[.]', link):
        return Archive(link, source, fake, unreliable)

    if re.match('^https?://web[.]archive[.]', link):
        return WebArchive(link, source, fake, unreliable)

    return DefaultArticleScraper(link, source, fake)
