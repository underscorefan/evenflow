import asyncio
import time
import traceback
import uvloop

from aiohttp import ClientSession
from typing import Tuple
from evenflow import Conf
from evenflow.streams import consumers, producers
from evenflow.urlman import UrlSet
from evenflow.dbops import sources as db_sources, DatabaseCredentials


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

    article_rules = conf.load_rules_into(
        consumers.ArticleRules(
            lambda url, from_fake: unreliable.contains(url) if from_fake else reliable.contains(url)
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
        await producers.collect_links(send_channel=q[s], to_scrape=feeds, session=session)
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
