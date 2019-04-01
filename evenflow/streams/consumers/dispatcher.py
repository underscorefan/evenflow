import asyncio
import json

from typing import Optional, List, Dict, Tuple, Callable

import aiofiles
from aiohttp import TCPConnector, ClientSession
from newspaper.configuration import Configuration
from dirtyfunc import Either, Left, Right

from evenflow.scrapers.article import article_factory
from evenflow.streams.messages import ArticleExtended, Error, DataKeeper

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


class ArticleRules:
    def __init__(self, url_checker: Callable[[str, bool, bool], bool]):
        self.__url_checker = url_checker
        self.__custom_checks: List[Callable[[ArticleExtended], bool]] = []

    def add_check(self, check: Callable[[ArticleExtended], bool]):
        self.__custom_checks.append(check)

    def add_title_check(self, check: Callable[[str], bool]):
        self.add_check(lambda a: check(a.title))

    def add_text_check(self, check: Callable[[str], bool]):
        self.add_check(lambda a: check(a.text))

    def add_path_check(self, check: Callable[[str], bool]):
        self.add_check(lambda a: check(a.path))

    def add_url_check(self, check: Callable[[str], bool]):
        self.add_check(lambda a: check(a.actual_url))

    def url_is_valid(self, url: str, from_fake: bool, archived: bool) -> bool:
        return self.__url_checker(url, from_fake, archived)

    def pass_checks(self, a: ArticleExtended) -> bool:
        for check in self.__custom_checks:
            if not check(a):
                return False
        return True


class DispatcherSettings:
    def __init__(
            self,
            connector: TCPConnector,
            headers: Dict[str, str],
            newspaper_conf: Configuration,
            rules: ArticleRules,
            backup_path: Optional[str] = None,
            initial_state: Optional[Dict] = None,
            timeout: Optional[int] = 60
    ):
        self.connector = connector
        self.headers = headers
        self.newspaper_conf = newspaper_conf
        self.rules = rules
        self.backup_path = backup_path
        self.state = initial_state
        self.timeout = timeout

    def make_session(self) -> ClientSession:
        return ClientSession(connector=self.connector, headers=self.headers)

    def make_backup_manager(self) -> BackupManager:
        return BackupManager(self.backup_path, self.state)

    def unpack_check(self, url: str, item: Tuple[str, bool]):
        _, from_fake = item
        return self.rules.url_is_valid(url, from_fake, False)


class DefaultDispatcher(DispatcherSettings):
    def __init__(
            self,
            rules: ArticleRules,
            backup_path: Optional[str] = None,
            initial_state: Optional[Dict] = None
    ):
        super().__init__(
            connector=TCPConnector(limit_per_host=LIMIT_PER_HOST),
            headers=firefox,
            rules=rules,
            backup_path=backup_path,
            initial_state=initial_state,
            newspaper_conf=newspaper_config()
        )


class DispatcherQueues:
    def __init__(self, links: asyncio.Queue, storage: asyncio.Queue, error: asyncio.Queue):
        self.links = links
        self.storage = storage
        self.error = error

    async def send_error(self, error: Error):
        await self.error.put(error)

    async def send_articles(self, articles: List[ArticleExtended]):
        print(f"sending {len(articles)}")
        await self.storage.put(articles)

    async def receive_links(self) -> DataKeeper:
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
    def __init__(self, rules: ArticleRules):
        self.__duplicate_checker = DuplicateChecker()
        self.__list: List[ArticleExtended] = []
        self.rules = rules

    def add(self, a: ArticleExtended) -> Optional[str]:
        if not self.__duplicate_checker.is_valid(a):
            return None

        if a.archived and not self.rules.url_is_valid(a.actual_url, a.fake, a.archived):
            return None

        if not self.rules.pass_checks(a):
            return None

        self.__list.append(a)
        return f"{a.actual_url} appended"

    @property
    def get(self) -> List[ArticleExtended]:
        return self.__list

    def free_resources(self):
        self.__list = []
        self.__duplicate_checker = DuplicateChecker()


class CoroCreator:
    def __init__(self, session: ClientSession, newspaper_conf: Configuration, timeout: Optional[int]):
        self.session = session
        self.newspaper_conf = newspaper_conf
        self.timeout = timeout

    async def new_coro(self, link: str, item: Tuple[str, bool]) -> Either[Error, ArticleExtended]:
        source, fake = item
        try:
            scraper = article_factory(link=link, source=source, fake=fake)
            article_wr = await scraper.get_data(self.session, self.newspaper_conf, self.timeout)

            if article_wr.empty:
                err = article_wr.on_left()
                raise err if err else TimeoutError(f"request for {link} scraped from {source} took too much time")

            return Right(article_wr.on_right())

        except Exception as e:
            return Left(Error.from_exception(exc=e, url=link, source=source, fake=fake))


async def dispatch_links(conf: DispatcherSettings, queues: DispatcherQueues):
    backup_manager = conf.make_backup_manager()

    async with conf.make_session() as session:
        coro_creator = CoroCreator(session=session, newspaper_conf=conf.newspaper_conf, timeout=conf.timeout)
        article_list = ArticleListManager(conf.rules)

        while True:
            links = await queues.receive_links()
            extracted_data = links.filter(lambda url, item: conf.unpack_check(url, item))

            results = asyncio.gather(*[coro_creator.new_coro(link, item) for link, item in extracted_data.items])

            for result in await results:
                msg = result.map(lambda article: article_list.add(article))
                msg.on_right(lambda m: print(m))

                await result.on_left_awaitable(lambda e: queues.send_error(e))

            await queues.send_articles(article_list.get)
            await backup_manager.store(extracted_data.state)

            article_list.free_resources()

            queues.mark_links()
