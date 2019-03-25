import re

from .article_scraper import ArticleScraper, Archive, WebArchive, DefaultArticleScraper


def article_factory(link: str, source: str, fake: bool) -> ArticleScraper:
    if re.match('^https?://archive[.]', link):
        return Archive(link, source, fake)

    if re.match('^https?://web[.]archive[.]', link):
        return WebArchive(link, source, fake)

    return DefaultArticleScraper(link, source, fake)
