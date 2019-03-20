import asyncio
import json

from typing import Optional, List, Dict, Tuple

import aiofiles
from aiohttp import TCPConnector, ClientSession
from newspaper.configuration import Configuration
from dirtyfunc import Either, Left, Right

from evenflow.scraper import create_article_scraper
from evenflow.messages import ArticleExtended, Error, ExtractedDataKeeper
from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.helpers.req.headers import firefox


LIMIT_PER_HOST = 2


def newspaper_config() -> Configuration:
    conf = Configuration()
    conf.MAX_TITLE = 500
    return conf


class BackupManager:
    __INDENT = 2

    def __init__(self, backup_path: Optional[str], initial_state: Optional[Dict]):
        self.path = backup_path
        self.state = {} if not initial_state else initial_state

    async def store(self, add: Dict[str, Dict]):
        if self.path is not None:
            self.state = {**self.state, **add}
            async with aiofiles.open(self.path, mode='w') as f:
                await f.write(json.dumps(self.state, indent=self.__INDENT))


class ArticleStreamConfiguration:
    def __init__(
            self,
            connector: TCPConnector,
            headers: Dict[str, str],
            newspaper_conf: Configuration,
            backup_path: Optional[str] = None,
            initial_state: Optional[Dict] = None
    ):
        self.connector = connector
        self.headers = headers
        self.backup_path = backup_path
        self.newspaper_conf = newspaper_conf
        self.state = initial_state

    def make_session(self) -> ClientSession:
        return ClientSession(connector=self.connector, headers=self.headers)

    def make_backup_manager(self) -> BackupManager:
        return BackupManager(self.backup_path, self.state)


class DefaultArticleStreamConf(ArticleStreamConfiguration):
    def __init__(self, backup_path: Optional[str] = None, initial_state: Optional[Dict] = None):
        super().__init__(
            connector=TCPConnector(limit_per_host=LIMIT_PER_HOST),
            headers=firefox,
            backup_path=backup_path,
            initial_state=initial_state,
            newspaper_conf=newspaper_config()
        )


class ArticleStreamQueues:
    def __init__(self, links: asyncio.Queue, storage: asyncio.Queue, error: asyncio.Queue, verbose: bool = True):
        self.links = links
        self.storage = storage
        self.error = error
        self.verbose = verbose

    async def create_and_send_error(self, message: str, link: str, source: str):
        await self.error.put(Error(msg=message, url=link, source=source))

    async def send_error(self, error: Error):
        await self.error.put(error)

    async def send_articles(self, articles: List[ArticleExtended]):
        if self.verbose:
            print(f"sending {len(articles)}")
        await self.storage.put(articles)

    async def receive_links(self) -> ExtractedDataKeeper:
        return await self.links.get()

    def mark_links(self):
        self.links.task_done()


class ArticleChecker:
    def __init__(self):
        self.duplicate_title = set()
        self.duplicate_url = set()

    def is_valid(self, a: Optional[ArticleExtended]) -> bool:

        if a is None:
            return False

        title = a.title.strip()
        url = a.actual_url.strip()

        if title in self.duplicate_title:
            return False

        if url in self.duplicate_url:
            return False

        self.duplicate_url.add(url)
        self.duplicate_title.add(title)

        return True

    def flush_dicts(self):
        self.duplicate_title = set()
        self.duplicate_url = set()


class ArticleListManager:
    def __init__(self, verbose=True):
        self._checker = ArticleChecker()
        self._list: List[ArticleExtended] = []
        self._verbose = verbose

    def add(self, a: ArticleExtended) -> Optional[str]:
        if self._checker.is_valid(a):
            self._list.append(a)
            return f"{a.actual_url} appended" if self._verbose else None
        return None

    @property
    def get(self) -> List[ArticleExtended]:
        return self._list


class CoroCreator:
    def __init__(self, unrel: UnreliableSet, session: ClientSession, newspaper_conf: Configuration):
        self.unrel = unrel
        self.session = session
        self.newspaper_conf = newspaper_conf

    async def new_coro(self, link: str, item: Tuple[str, bool]) -> Either[Error, ArticleExtended]:
        source, fake = item
        try:
            scraper = create_article_scraper(link=link, source=source, unreliable=self.unrel, fake=fake)
            return Right(await scraper.get_data(self.session, self.newspaper_conf))
        except Exception as e:
            return Left(Error.from_exception(exc=e, url=link, source=source))


async def handle_links(stream_conf: ArticleStreamConfiguration, queues: ArticleStreamQueues, unreliable: UnreliableSet):
    backup_manager = stream_conf.make_backup_manager()

    async with stream_conf.make_session() as session:
        coro_creator = CoroCreator(unrel=unreliable, session=session, newspaper_conf=stream_conf.newspaper_conf)
        while True:
            link_container = await queues.receive_links()
            article_list = ArticleListManager()
            results = asyncio.gather(*[coro_creator.new_coro(link, item) for link, item in link_container.items])

            for result in await results:
                result.map(lambda article: article_list.add(article)).on_right(lambda m: print(m))
                await result.on_left_awaitable(lambda e: queues.send_error(e))

            await queues.send_articles(article_list.get)
            await backup_manager.store(link_container.backup)
            queues.mark_links()
