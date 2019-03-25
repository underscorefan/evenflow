import asyncio
import json

from typing import Optional, List, Dict, Tuple, Callable

import aiofiles
from aiohttp import TCPConnector, ClientSession
from newspaper.configuration import Configuration
from dirtyfunc import Either, Left, Right

from evenflow.scrapers.article import article_factory
from evenflow.streams.messages import ArticleExtended, Error, ExtractedDataKeeper

LIMIT_PER_HOST = 2

firefox = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0"}


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


class DispatcherSettings:
    def __init__(
            self,
            connector: TCPConnector,
            headers: Dict[str, str],
            newspaper_conf: Configuration,
            extract_if: Callable[[str, bool], bool],
            backup_path: Optional[str] = None,
            initial_state: Optional[Dict] = None
    ):
        self.connector = connector
        self.headers = headers
        self.newspaper_conf = newspaper_conf
        self.extract_if = extract_if
        self.backup_path = backup_path
        self.state = initial_state

    def make_session(self) -> ClientSession:
        return ClientSession(connector=self.connector, headers=self.headers)

    def make_backup_manager(self) -> BackupManager:
        return BackupManager(self.backup_path, self.state)

    def unpack_extract_if(self, url: str, item: Tuple[str, bool]):
        _, from_fake = item
        return self.extract_if(url, from_fake)


class DefaultDispatcher(DispatcherSettings):
    def __init__(
            self,
            extract_if: Callable[[str, bool], bool],
            backup_path: Optional[str] = None,
            initial_state: Optional[Dict] = None
    ):
        super().__init__(
            connector=TCPConnector(limit_per_host=LIMIT_PER_HOST),
            headers=firefox,
            extract_if=extract_if,
            backup_path=backup_path,
            initial_state=initial_state,
            newspaper_conf=newspaper_config()
        )


class DispatcherQueues:
    def __init__(self, links: asyncio.Queue, storage: asyncio.Queue, error: asyncio.Queue):
        self.links = links
        self.storage = storage
        self.error = error

    async def create_and_send_error(self, message: str, link: str, source: str):
        await self.error.put(Error(msg=message, url=link, source=source))

    async def send_error(self, error: Error):
        await self.error.put(error)

    async def send_articles(self, articles: List[ArticleExtended]):
        print(f"sending {len(articles)}")
        await self.storage.put(articles)

    async def receive_links(self) -> ExtractedDataKeeper:
        return await self.links.get()

    def mark_links(self):
        self.links.task_done()


class DuplicateChecker:
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
    def __init__(self, url_checker: Callable[[str, bool], bool]):
        self.__duplicate_checker = DuplicateChecker()
        self.__list: List[ArticleExtended] = []
        self.__url_checker = url_checker

    def add(self, a: ArticleExtended) -> Optional[str]:
        if a.actual_url != a.url_to_visit and not self.__url_checker(a.actual_url, a.fake):
            return None

        if not self.__duplicate_checker.is_valid(a):
            return None

        self.__list.append(a)
        return f"{a.actual_url} appended"

    @property
    def get(self) -> List[ArticleExtended]:
        return self.__list


class CoroCreator:
    def __init__(
            self,
            extract_if_function: Callable[[str, bool], bool],
            session: ClientSession,
            newspaper_conf: Configuration
    ):
        self.extract_if_function = extract_if_function
        self.session = session
        self.newspaper_conf = newspaper_conf

    async def new_coro(self, link: str, item: Tuple[str, bool]) -> Either[Error, ArticleExtended]:
        source, fake = item
        try:
            scraper = article_factory(link=link, source=source, fake=fake)
            maybe_article = await scraper.get_data(self.session, self.newspaper_conf)

            if maybe_article.empty:
                return Left(maybe_article.on_left(lambda exc: Error.from_exception(exc=exc, url=link, source=source)))

            return Right(maybe_article.on_right())

        except Exception as e:
            return Left(Error.from_exception(exc=e, url=link, source=source))


async def dispatch_links(conf: DispatcherSettings, queues: DispatcherQueues):
    backup_manager = conf.make_backup_manager()

    async with conf.make_session() as session:
        coro_creator = CoroCreator(
            extract_if_function=conf.extract_if,
            session=session,
            newspaper_conf=conf.newspaper_conf
        )

        while True:
            extracted_data = (await queues.receive_links())\
                .filter(lambda url, item: conf.unpack_extract_if(url, item))

            article_list = ArticleListManager(conf.extract_if)
            results = asyncio.gather(*[coro_creator.new_coro(link, item) for link, item in extracted_data.items])

            for result in await results:
                result.map(lambda article: article_list.add(article)).on_right(lambda m: print(m))
                await result.on_left_awaitable(lambda e: queues.send_error(e))

            await queues.send_articles(article_list.get)
            await backup_manager.store(extracted_data.backup)

            queues.mark_links()
