import re

from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.scraper import Scraper
from evenflow.scraper.scraper import Archive, WebArchive, ArticleScraper


def create_article_scraper(link: str, source: str, fake: bool, unreliable: UnreliableSet) -> Scraper:
    if re.match('^https?://archive[.]', link):
        return Archive(link, source, fake, unreliable)

    if re.match('^https?://web[.]archive[.]', link):
        return WebArchive(link, source, fake, unreliable)

    return ArticleScraper(link, source, fake)
