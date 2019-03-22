import asyncio
import time
import traceback
from typing import List, Tuple

import uvloop
from aiohttp import ClientSession

from evenflow import Conf

from evenflow.scrapers.feed import FeedScraper
from evenflow.streams.consumers import dispatch_links, DefaultDispatcher, DispatcherQueues, store_articles, store_errors
from evenflow.streams.producers import LinkProducerSettings, collect_links
from evenflow.urlman import UrlSet
from evenflow.dbops import sources as db_sources, DatabaseCredentials


def from_file(path: str):
    pass


def from_list(sources: List[FeedScraper]):
    pass


async def make_url_sets(cred: DatabaseCredentials) -> Tuple[UrlSet, UrlSet]:
    records = await cred.do_with_connection(lambda conn: db_sources.select_sources(conn))
    reliable, unreliable = UrlSet(), UrlSet()
    true_labels, fake_labels = {'very high', 'high'}, {'mixed', 'low', 'insane stuff', 'satire', 'very low'}
    for record in records:

        if record['factual_reporting'] in true_labels:
            reliable.add(record['url'], netloc=False)
            continue

        if record['factual_reporting'] in fake_labels:
            unreliable.add(record['url'], netloc=False)

    unreliable.add_multiple(
        urls=[f"archive.{dom}" for dom in ["is", "fo", "today"]] + ["web.archive.org"],
        netloc=False
    )

    return reliable, unreliable


async def asy_main(loop: asyncio.events, conf: Conf) -> float:
    feeds = conf.load_sources()

    if not feeds or len(feeds) == 0:
        print("no sources to begin with")
        return 0.0

    s, a, e = "sources", "articles", "errors"
    q = {name: asyncio.Queue(loop=loop) for name in [s, a, e]}

    print("about to create pool")
    db_cred = conf.setupdb()

    pg_pool = await db_cred.make_pool()
    reliable, unreliable = await make_url_sets(db_cred)

    link_producers_settings = LinkProducerSettings(
        unrel=unreliable,
        send_channel=q[s]
    )

    futures = [
        dispatch_links(
            stream_conf=DefaultDispatcher(conf.backup_file_path, conf.initial_state),
            queues=DispatcherQueues(links=q[s], storage=q[a], error=q[e]),
            unreliable=unreliable
        ),
        store_articles(
            pool=pg_pool,
            storage_queue=q[a],
            error_queue=q[e]
        ),
        store_errors(
            pool=pg_pool,
            error_queue=q[e]
        )
    ]

    consumer_jobs = [asyncio.ensure_future(future, loop=loop) for future in futures]

    start_time = time.perf_counter()
    async with ClientSession() as session:
        await collect_links(settings=link_producers_settings, to_scrape=feeds, session=session)
        scrape_time = time.perf_counter() - start_time

    for k in q:
        await q[k].join()

    for consumer in consumer_jobs:
        consumer.cancel()

    print("about to close pool")
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
