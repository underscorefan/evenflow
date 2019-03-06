import asyncio

from typing import Optional, List, Dict
from aiohttp import TCPConnector, ClientSession
from newspaper.configuration import Configuration
from evenflow.ade import scraper_factory
from evenflow.helpers.check.article_checker import ArticleChecker
from evenflow.helpers.file import asy_write_json
from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.helpers.req.headers import firefox
from evenflow.helpers.exc import get_name
from evenflow.messages import LinkContainer, ArticleExtended, Error


def newspaper_config() -> Configuration:
    conf = Configuration()
    conf.MAX_TITLE = 500
    return conf


class BackupManager:
    def __init__(self, backup_path: Optional[str]):
        self.path = backup_path
        self.state = {}

    async def store(self, add: Dict[str, str]):
        if self.path is not None:
            self.state = {**self.state, **add}
            await asy_write_json(path=self.path, obj=self.state)


class ArticleStreamConfiguration:
    def __init__(
            self,
            connector: TCPConnector,
            headers: Dict[str, str],
            newspaper_conf: Configuration,
            backup_path: Optional[str] = None
    ):
        self.connector = connector
        self.headers = headers
        self.backup_path = backup_path
        self.newspaper_conf = newspaper_conf

    def make_session(self) -> ClientSession:
        return ClientSession(connector=self.connector, headers=self.headers)

    def make_backup_manager(self) -> BackupManager:
        return BackupManager(self.backup_path)


class DefaultArticleStreamConf(ArticleStreamConfiguration):
    def __init__(self, backup_path: Optional[str] = None):
        super().__init__(
            connector=TCPConnector(limit_per_host=1),
            headers=firefox,
            backup_path=backup_path,
            newspaper_conf=newspaper_config()
        )


class ArticleStreamQueues:
    def __init__(self, links: asyncio.Queue, storage: asyncio.Queue, error: asyncio.Queue, verbose: bool = True):
        self.links = links
        self.storage = storage
        self.error = error
        self.verbose = verbose

    async def send_error(self, message: str, link: str, source: str):
        await self.error.put(Error(msg=message, url=link, source=source))

    async def send_articles(self, articles: List[ArticleExtended]):
        if self.verbose:
            print(f"sending {len(articles)}")
        await self.storage.put(articles)

    async def receive_links(self) -> LinkContainer:
        return await self.links.get()

    def mark_links(self):
        self.links.task_done()


class ArticleContainer:
    def __init__(self, verbose=True):
        self._checker = ArticleChecker()
        self._list: List[ArticleExtended] = []
        self._verbose = verbose

    def add_article(self, a: ArticleExtended) -> Optional[str]:
        if self._checker.is_valid(a):
            self._list.append(a)
            return f"{a.actual_url} appended" if self._verbose else None
        return None

    def get_articles(self) -> List[ArticleExtended]:
        return self._list


async def handle_links(sc: ArticleStreamConfiguration, queues: ArticleStreamQueues, unreliable: UnreliableSet):
    backup_manager = sc.make_backup_manager()
    async with sc.make_session() as session:
        while True:
            link_container = await queues.receive_links()
            article_container = ArticleContainer()
            for link, item in link_container.items():
                source, fake = item
                try:
                    scraper = scraper_factory(link=link, source=source, unreliable=unreliable, fake=fake)
                    msg = article_container.add_article(await scraper.get_data(session, sc.newspaper_conf))
                    if msg is not None:
                        print(msg)
                except Exception as e:
                    await queues.send_error(message=get_name(e), link=link, source=source)
            await queues.send_articles(article_container.get_articles())
            await backup_manager.store(link_container.backup)
            queues.mark_links()


async def default_handle_links(
        links: asyncio.Queue,
        storage: asyncio.Queue,
        error: asyncio.Queue,
        backup_path: str,
        unrel: UnreliableSet
):
    stream_conf = DefaultArticleStreamConf(backup_path=backup_path)
    stream_queue = ArticleStreamQueues(links=links, storage=storage, error=error)
    await handle_links(stream_conf, stream_queue, unrel)
