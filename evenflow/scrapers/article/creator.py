import re

from evenflow.urlman import UrlSet
from .scraper import Scraper, Archive, WebArchive, ArticleScraper


def create_article_scraper(link: str, source: str, fake: bool, unreliable: UrlSet) -> Scraper:
    if re.match('^https?://archive[.]', link):
        return Archive(link, source, fake, unreliable)

    if re.match('^https?://web[.]archive[.]', link):
        return WebArchive(link, source, fake, unreliable)

    return ArticleScraper(link, source, fake)
