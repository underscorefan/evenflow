import asyncio

from messages import LinkContainer
from .source import SourceSpider
from typing import List, ItemsView, Tuple, Dict
from aiohttp import ClientSession
from collections import defaultdict
from helpers.func import mmap
from helpers.hostracker import HostTracker
from helpers.unreliableset import UnreliableSet
from helpers.req import maintain_netloc


class SourceManager:
    def __init__(self, sources: List[SourceSpider], tracker: HostTracker, unrel: UnreliableSet):
        self.original_sources = sources
        self.tracker = tracker
        self.unrel = unrel

    async def fetch_articles(self, session: ClientSession, q: asyncio.Queue):
        sources = self.original_sources
        while len(sources) > 0:
            coroutines = [source.search_for_links(session) for source in sources]

            all_links = dict()
            backup = dict()
            sources: List[SourceSpider] = []

            for result in await asyncio.gather(*coroutines):
                mmap(result.next_spider, sources.append)
                all_links = {
                    **all_links,
                    **({k: v for k, v in result.links.items() if self.unrel.contains(k)})
                }
                backup[result.old_spider.name] = result.old_spider.to_dict()

            await q.put(LinkContainer(links=all_links, backup=backup))

    async def __group_by_host(self, links: Dict[str, str]) -> Tuple[ItemsView[str, set], set]:
        d = defaultdict(set)
        errors = set()
        for link, from_article in links.items():
            netloc = maintain_netloc(link)
            if not self.unrel.contains(url=netloc, netloc=False):
                continue
            k = await self.tracker.get_ip(url=netloc, parse=False)
            if k is not None:
                d[k].add((link, from_article))
            else:
                errors.add((link, from_article))
        return d.items(), errors

    def store_tracker(self, path: str):
        self.tracker.store_dict(path=path)
