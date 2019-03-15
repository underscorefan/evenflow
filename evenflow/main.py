import asyncio
import uvloop
import time
from aiohttp import ClientSession

from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.consumers.article import (
    handle_links,
    DefaultArticleStreamConf,
    ArticleStreamQueues
)
from evenflow.consumers.pg import (
    store_errors,
    store_articles
)
from evenflow.readconf import (
    Conf,
    conf_from_cli
)

from evenflow.producers import (
    produce_links,
    LinkProducerSettings
)


def create_unreliable(conf: Conf) -> UnreliableSet:
    unreliable = conf.load_unreliable()

    unreliable.add_multiple(
        urls=[f"archive.{dom}" for dom in ["is", "fo", "today"]]+["web.archive.org"],
        netloc=False
    )

    return unreliable


async def main(loop: asyncio.events, conf: Conf) -> float:
    unreliable_set = create_unreliable(conf=conf)

    readers = conf.load_sources()

    if not readers or len(readers) == 0:
        print("no sources to begin with")
        return 0.0

    s, a, e = ("sources", "articles", "errors")
    q = {name: asyncio.Queue(loop=loop) for name in [s, a, e]}

    link_producers_settings = LinkProducerSettings(
        tracker=conf.load_host_tracker(loop=loop),
        unrel=unreliable_set,
        send_channel=q[s]
    )

    print("about to create pool")
    pg_pool = await conf.setupdb().make_pool()

    futures = [
        handle_links(
            stream_conf=DefaultArticleStreamConf(conf.backup_file_path, conf.initial_state),
            queues=ArticleStreamQueues(links=q[s], storage=q[a], error=q[e]),
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

    consumer_jobs = [asyncio.ensure_future(future, loop=event_loop) for future in futures]

    start_time = time.perf_counter()
    async with ClientSession() as session:
        await produce_links(settings=link_producers_settings, to_read=readers, session=session)
        scrape_time = time.perf_counter() - start_time

    for k in q:
        await q[k].join()

    for consumer in consumer_jobs:
        consumer.cancel()

    print("about to close pool")
    await pg_pool.close()
    print("pool closed")

    return scrape_time

if __name__ == '__main__':
    c = conf_from_cli()

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    event_loop = asyncio.get_event_loop()
    try:
        exec_time = event_loop.run_until_complete(main(loop=event_loop, conf=c))
        print(f"job executed in {exec_time:0.2f} seconds.")
    except Exception as err:
        print(err)
    finally:
        event_loop.close()
