import asyncio
import time
import traceback
import uvloop

from asyncpg.pool import Pool
from typing import Optional, List
from functools import partial
from aiohttp import ClientSession
from evenflow import Conf
from evenflow.streams import consumers, producers
from evenflow.urlman import LabelledSources
from evenflow.dbops import queries, DatabaseCredentials
from evenflow.scrapers.feed import FeedScraper

HIGH, LOW, MIXED, ARCHIVE = "high", "low", "mixed", "archive"
S, A, E, D = "sources", "articles", "errors", "delete"


class ConnectionManager:
    def __init__(self, db_credentials: DatabaseCredentials):
        self.db_credentials = db_credentials
        self.__pool: Optional[Pool] = None

    @property
    async def article_rules(self) -> consumers.ArticleRules:
        labelled_sources = await self.make_url_dict()
        add_url_partial = partial(self.add_url, labelled_sources)
        return consumers.ArticleRules(lambda url, from_fake, archived: add_url_partial(url, from_fake, archived))

    async def make_url_dict(self) -> LabelledSources:
        records = await self.db_credentials.do_with_connection(lambda c: queries.select_sources(c))
        labelled_sources = LabelledSources(False)
        true_labels, fake_labels = {'very high', 'high'}, {'low', 'insane stuff', 'satire', 'very low'}

        for record in records:
            label, url = record['factual_reporting'], record["url"]

            if label == MIXED:
                labelled_sources[url] = MIXED
                continue

            if label in true_labels:
                labelled_sources[url] = HIGH
                continue

            if label in fake_labels:
                labelled_sources[url] = LOW

        for arch in [f"archive.{dom}" for dom in ["is", "fo", "today"]] + ["web.archive.org"]:
            labelled_sources[arch] = ARCHIVE

        return labelled_sources.strip(True)

    @property
    async def pool(self) -> Pool:
        if self.__pool is None:
            print("about to create pool")
            self.__pool = await self.db_credentials.make_pool()
        return self.__pool

    async def close_pool(self):
        if self.__pool is not None:
            await self.__pool.close()

    async def delete_errors(self):
        await self.db_credentials.do_with_connection(lambda conn: queries.delete_errors(conn))

    @staticmethod
    def add_url(labelled_sources: LabelledSources, url: str, from_fake: bool, from_archive: bool) -> bool:
        label = labelled_sources[url]

        if label == HIGH:
            return not from_fake

        if label == LOW:
            return from_fake

        if label == MIXED:
            return from_fake and from_archive

        return label == ARCHIVE and from_fake and not from_archive


async def asy_main(loop: asyncio.events, conf: Conf) -> float:
    maybe_feeds, maybe_rs = conf.load_sources(), conf.load_reddit_settings()

    if maybe_feeds.empty and maybe_rs.empty and not conf.restore:
        print("no sources to begin with")
        return 0.0

    feeds: List[FeedScraper] = maybe_feeds.on_right()
    reddit_settings: producers.RedditSettings = maybe_rs.on_right()

    q = {name: asyncio.Queue(loop=loop) for name in ([S, A, E, D] if conf.restore else [S, A, E])}

    bootstrap = ConnectionManager(db_credentials=conf.setupdb())

    dispatcher_conf = consumers.DefaultDispatcher(
        backup_path=conf.backup_file_path if not conf.restore else None,
        initial_state=conf.initial_state if not conf.restore else None,
        timeout=60 if not conf.restore else 1800,
        rules=conf.load_rules_into(await bootstrap.article_rules)
    )

    dispatcher_queues = consumers.DispatcherQueues(links=q[S], storage=q[A], error=q[E])

    futures = [
        consumers.dispatch_links(conf=dispatcher_conf, queues=dispatcher_queues),
        consumers.store_articles(
            pool=await bootstrap.pool, storage_queue=q[A], error_queue=q[E], delete_queue=q.get(D)
        ),
        consumers.store_errors(pool=await bootstrap.pool, error_queue=q[E])
    ]

    if conf.restore:
        futures.append(consumers.delete_errors(pool=await bootstrap.pool, delete=q[D]))

    consumer_jobs = [asyncio.ensure_future(future, loop=loop) for future in futures]

    start_time = time.perf_counter()

    if not conf.restore:
        async with ClientSession() as session:
            await producers.collect_links_html(send_channel=q[S], to_scrape=feeds, session=session)
        await producers.collect_links_reddit(send_channel=q[S], rsm=reddit_settings)
    else:
        for label in [True, False]:
            await producers.restore_errors(send=q[S], db_cred=bootstrap.db_credentials, fake=label)

    scrape_time = time.perf_counter() - start_time

    for k in q:
        await q[k].join()

    for consumer in consumer_jobs:
        consumer.cancel()

    print("delete duplicate errors")
    print(await bootstrap.delete_errors())
    await bootstrap.close_pool()
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
