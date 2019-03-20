import asyncio
import time
import traceback

import uvloop
from aiohttp import ClientSession

from evenflow.streams.consumers import (
    dispatch_links,
    DefaultDispatcher,
    DispatcherQueues,
    store_errors,
    store_articles
)

from evenflow.urlman import UrlSet
from evenflow.streams.producers import (
    collect_links,
    LinkProducerSettings
)
from evenflow.read_conf import (
    Conf,
    conf_from_cli
)


def create_unreliable(conf: Conf) -> UrlSet:
    unreliable = conf.load_unreliable()

    unreliable.add_multiple(
        urls=[f"archive.{dom}" for dom in ["is", "fo", "today"]]+["web.archive.org"],
        netloc=False
    )

    return unreliable


async def asy_main(loop: asyncio.events, conf: Conf) -> float:
    unreliable_set = create_unreliable(conf=conf)

    readers = conf.load_sources()

    if not readers or len(readers) == 0:
        print("no sources to begin with")
        return 0.0

    s, a, e = ("sources", "articles", "errors")
    q = {name: asyncio.Queue(loop=loop) for name in [s, a, e]}

    link_producers_settings = LinkProducerSettings(
        unrel=unreliable_set,
        send_channel=q[s]
    )

    print("about to create pool")
    pg_pool = await conf.setupdb().make_pool()

    futures = [
        dispatch_links(
            stream_conf=DefaultDispatcher(conf.backup_file_path, conf.initial_state),
            queues=DispatcherQueues(links=q[s], storage=q[a], error=q[e]),
            unreliable=unreliable_set
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
        await collect_links(settings=link_producers_settings, to_read=readers, session=session)
        scrape_time = time.perf_counter() - start_time

    for k in q:
        await q[k].join()

    for consumer in consumer_jobs:
        consumer.cancel()

    print("about to close pool")
    await pg_pool.close()
    print("pool closed")

    return scrape_time


def run():
    c = conf_from_cli()
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


if __name__ == '__main__':
    run()
