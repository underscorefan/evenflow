import asyncio
import time
import traceback
import uvloop

from dirtyfunc import Either
from typing import Optional, List
from functools import partial
from aiohttp import ClientSession
from evenflow import Conf
from evenflow.streams import consumers, producers
from evenflow.urlman import LabelledSources
from evenflow.dbops import select as db_sources, DatabaseCredentials
from evenflow.scrapers.feed import FeedScraper

HIGH, LOW, MIXED, ARCHIVE = "high", "low", "mixed", "archive"


class Bootstrap:
    def __init__(self, loop: asyncio.events, c: Conf):
        self.loop = loop
        self.db_credentials: DatabaseCredentials = c.setupdb()
        self.feeds: Optional[List[FeedScraper]] = c.load_sources()
        self.maybe_reddit_settings: Either[Exception, producers.RedditSettings] = c.load_reddit_settings()


async def make_url_dict(cred: DatabaseCredentials) -> LabelledSources:
    records = await cred.do_with_connection(lambda c: db_sources.select_sources(c))
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
    feeds = conf.load_sources()
    maybe_rs = conf.load_reddit_settings()

    if maybe_rs.empty:
        print(maybe_rs.on_left())
        return 0.0
    reddit_settings: producers.RedditSettings = maybe_rs.on_right()

    if (not feeds or len(feeds) == 0) and len(reddit_settings.subreddits) == 0:
        print("no sources to begin with")
        return 0.0

    s, a, e = "sources", "articles", "errors"
    q = {name: asyncio.Queue(loop=loop) for name in [s, a, e]}

    print("about to create pool")
    db_cred = conf.setupdb()

    pg_pool = await db_cred.make_pool()

    labelled_sources = await make_url_dict(db_cred)
    add_url_partial = partial(add_url, labelled_sources)

    article_rules = conf.load_rules_into(
        consumers.ArticleRules(
            lambda url, from_fake, archived: add_url_partial(url, from_fake, archived)
        )
    )

    dispatcher_conf = consumers.DefaultDispatcher(
        backup_path=conf.backup_file_path,
        initial_state=conf.initial_state,
        rules=article_rules
    )

    dispatcher_queues = consumers.DispatcherQueues(links=q[s], storage=q[a], error=q[e])

    futures = [
        consumers.dispatch_links(conf=dispatcher_conf, queues=dispatcher_queues),
        consumers.store_articles(pool=pg_pool, storage_queue=q[a], error_queue=q[e]),
        consumers.store_errors(pool=pg_pool, error_queue=q[e])
    ]

    consumer_jobs = [asyncio.ensure_future(future, loop=loop) for future in futures]

    start_time = time.perf_counter()

    async with ClientSession() as session:
        await producers.collect_links_html(send_channel=q[s], to_scrape=feeds, session=session)
        await producers.collect_links_reddit(send_channel=q[s], rsm=reddit_settings)
        scrape_time = time.perf_counter() - start_time

    for k in q:
        await q[k].join()

    for consumer in consumer_jobs:
        consumer.cancel()

    await pg_pool.close()
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
