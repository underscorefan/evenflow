import asyncio

from typing import Optional, List, Dict, Tuple
from aiohttp import TCPConnector, ClientSession
from newspaper.configuration import Configuration
from dirtyfunc import Either, Left, Right

from evenflow.ade import scraper_factory
from evenflow.messages import LinkContainer, ArticleExtended, Error
from evenflow.helpers.check.article_checker import ArticleChecker
from evenflow.helpers.file import asy_write_json
from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.helpers.req.headers import firefox


LIMIT_PER_HOST = 2


def newspaper_config() -> Configuration:
    conf = Configuration()
    conf.MAX_TITLE = 500
    return conf


class BackupManager:
    def __init__(self, backup_path: Optional[str], initial_state: Optional[Dict]):
        self.path = backup_path
        self.state = {} if not initial_state else initial_state

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


class CoroCreator:
    def __init__(self, unrel: UnreliableSet, session: ClientSession, newspaper_conf: Configuration):
        self.unrel = unrel
        self.session = session
        self.newspaper_conf = newspaper_conf

    async def new_coro(self, link: str, item: Tuple[str, bool]) -> Either[Error, ArticleExtended]:
        source, fake = item
        try:
            scraper = scraper_factory(link=link, source=source, unreliable=self.unrel, fake=fake)
            return Right(await scraper.get_data(self.session, self.newspaper_conf))
        except Exception as e:
            return Left(Error.from_exception(exc=e, url=link, source=source))


async def handle_links(stream_conf: ArticleStreamConfiguration, queues: ArticleStreamQueues, unreliable: UnreliableSet):
    backup_manager = stream_conf.make_backup_manager()

    async with stream_conf.make_session() as session:
        coro_creator = CoroCreator(unrel=unreliable, session=session, newspaper_conf=stream_conf.newspaper_conf)
        while True:
            link_container = await queues.receive_links()
            article_container = ArticleContainer()
            results = asyncio.gather(*[coro_creator.new_coro(link, item) for link, item in link_container.items()])

            for result in await results:
                result: Either[Error, ArticleExtended] = result
                result.map(lambda article: article_container.add_article(article)).on_right(lambda m: print(m))
                await result.on_left_awaitable(lambda e: queues.send_error(e))

            await queues.send_articles(article_container.get_articles())
            await backup_manager.store(link_container.backup)
            queues.mark_links()
