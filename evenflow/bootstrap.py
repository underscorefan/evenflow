import asyncio
import time
import traceback
import uvloop
import abc

from asyncpg.pool import Pool
from typing import List, Dict, Awaitable
from functools import partial
from aiohttp import ClientSession
from dirtyfunc import Either, Left, Right
from evenflow import Conf
from evenflow.dbops import DatabaseCredentials
from evenflow.data_manager import DataManager
from evenflow.streams import consumers, producers
from evenflow.scrapers.feed import FeedScraper

S, A, E, D = "sources", "articles", "errors", "delete"


class Bootstrap(abc.ABC):
    def __init__(self, loop: asyncio.events, rules: consumers.ArticleRules):
        self.loop = loop
        self.q = self.__create_queues()
        self.dispatcher_settings = self.__create_dispatcher(rules)

    def __create_queues(self) -> Dict[str, asyncio.Queue]:
        return {name: asyncio.Queue(loop=self.loop) for name in [S, A, E]}

    def __create_dispatcher(self, rules: consumers.ArticleRules):
        return partial(consumers.DefaultDispatcher, loop=self.loop, rules=rules)

    def create_tasks(self, pool: Pool) -> List[Awaitable]:
        dispatcher_queues = consumers.DispatcherQueues(links=self.q[S], storage=self.q[A], error=self.q[E])
        return [
            consumers.dispatch_links(conf=self.dispatcher_settings(), queues=dispatcher_queues),
            consumers.store_articles(
                pool=pool, storage_queue=self.q[A], error_queue=self.q[E], delete_queue=self.q.get(D)
            ),
            consumers.store_errors(pool=pool, error_queue=self.q[E])
        ]

    @abc.abstractmethod
    def create_producers(self) -> List[Awaitable]:
        pass


class BootstrapScraper(Bootstrap):

    def __init__(
            self,
            loop: asyncio.events,
            rules: consumers.ArticleRules,
            backup_path: str,
            initial_state: Dict,
            feeds: List[FeedScraper],
            reddit_settings: producers.RedditSettings,
            session: ClientSession
    ):
        super().__init__(loop=loop, rules=rules)
        self.dispatcher_settings = partial(
            self.dispatcher_settings,
            backup_path=backup_path,
            initial_state=initial_state,
            timeout=60
        )
        self.feeds = feeds
        self.rsm = reddit_settings
        self.ssn = session

    def create_producers(self) -> List[Awaitable]:
        return [
            producers.collect_links_html(send_channel=self.q[S], to_scrape=self.feeds, session=self.ssn),
            producers.collect_links_reddit(send_channel=self.q[S], rsm=self.rsm)
        ]


class BootstrapRestorer(Bootstrap):

    def __init__(self, loop: asyncio.events, rules: consumers.ArticleRules, db_cred: DatabaseCredentials):
        super().__init__(loop, rules)
        self.q[D] = asyncio.Queue(loop=loop)
        self.db_cred = db_cred
        self.dispatcher_settings = partial(self.dispatcher_settings, timeout=189)

    def create_tasks(self, pool: Pool) -> List[Awaitable]:
        to_ret = super().create_tasks(pool)
        to_ret.append(consumers.delete_errors(pool=pool, delete=self.q[D]))
        return to_ret

    def create_producers(self) -> List[Awaitable]:
        return [
            producers.restore_errors(send_channel=self.q[S], db_cred=self.db_cred, fake=label)
            for label in [True, False]
        ]


def bootstrapper(
        conf: Conf,
        loop: asyncio.events,
        rules: consumers.ArticleRules,
        session: ClientSession
) -> Either[Exception, Bootstrap]:

    maybe_feeds, maybe_rs = conf.load_sources(), conf.load_reddit_settings()
    if maybe_feeds.empty and maybe_rs.empty and not conf.restore:
        return Left[Exception](ValueError("no sources to begin with"))

    if conf.restore:
        return Right[Bootstrap](BootstrapRestorer(loop=loop, db_cred=conf.setupdb(), rules=rules))

    return Right[Bootstrap](
        BootstrapScraper(
            loop=loop,
            rules=rules,
            feeds=maybe_feeds.on_right(),
            reddit_settings=maybe_rs.on_right(),
            backup_path=conf.backup_file_path,
            initial_state=conf.initial_state,
            session=session
        )
    )


async def asy_main(loop: asyncio.events, conf: Conf) -> float:
    data_manager = DataManager(db_credentials=conf.setupdb())
    article_rules = conf.load_rules_into(await data_manager.article_rules)

    async with ClientSession(loop=loop) as session:
        maybe_start = bootstrapper(conf, loop, rules=article_rules, session=session)
        if maybe_start.empty:
            print(maybe_start.on_left())
            return 0.0

        bootstrap: Bootstrap = maybe_start.on_right()
        jobs = [asyncio.ensure_future(task, loop=loop) for task in bootstrap.create_tasks(await data_manager.pool)]

        start_time = time.perf_counter()
        for producer in bootstrap.create_producers():
            await producer

        scrape_time = time.perf_counter() - start_time

        for queue in bootstrap.q.values():
            await queue.join()

        for job in jobs:
            job.cancel()

        print("delete duplicate errors")
        await data_manager.delete_errors()
        await data_manager.close_pool()
        print("pool closed")

        return scrape_time


def run(c: Conf):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    event_loop = asyncio.get_event_loop()
    try:
        exec_time = event_loop.run_until_complete(asy_main(loop=event_loop, conf=c))
        print(f"job executed in {exec_time:0.2f} seconds.")
    except Exception as err:
        print(f'asy_main {err}')
        print(traceback.format_exc())
    finally:
        event_loop.close()
