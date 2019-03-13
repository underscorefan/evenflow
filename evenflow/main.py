import asyncio
import uvloop
import time
from aiohttp import ClientSession
from evenflow import consumers
from evenflow.fci import SourceManager
from evenflow.readconf import Conf, conf_from_cli
from evenflow.helpers.unreliableset import UnreliableSet


def create_unreliable(conf: Conf) -> UnreliableSet:
    unreliable = conf.load_unreliable()

    unreliable.add_multiple(
        urls=[f"archive.{dom}" for dom in ["is", "fo", "today"]]+["web.archive.org"],
        netloc=False
    )

    return unreliable


async def sources_layer(loop: asyncio.events, conf: Conf, unreliable: UnreliableSet, sq: asyncio.Queue) -> float:
    sources = conf.load_sources()
    if not sources or len(sources) == 0:
        print("no sources to begin with")
        return 0.0

    source_man = SourceManager(sources=sources, tracker=conf.load_host_tracker(loop), unrel=unreliable)
    s = time.perf_counter()

    async with ClientSession() as session:
        await source_man.fetch_articles(session, sq)
        source_man.store_tracker(conf.host_cache)
        return time.perf_counter() - s


async def main(loop: asyncio.events, conf: Conf) -> float:
    unreliable_set = create_unreliable(conf=conf)

    s, a, e = ("sources", "articles", "errors")
    q = {name: asyncio.Queue(loop=loop) for name in [s, a, e]}

    print("about to create pool")
    pg_pool = await conf.setupdb().make_pool()

    futures = [
        consumers.article.default_handle_links(
            links=q[s],
            storage=q[a],
            error=q[e],
            backup_path=conf.backup_file_path,
            unrel=unreliable_set
        ),
        consumers.pg.store_articles(
            pool=pg_pool,
            storage_queue=q[a],
            error_queue=q[e]
        ),
        consumers.pg.store_errors(
            pool=pg_pool,
            error_queue=q[e]
        )
    ]

    consumer_jobs = [asyncio.ensure_future(future, loop=event_loop) for future in futures]

    scrape_time = await sources_layer(loop=loop, conf=conf, sq=q[s], unreliable=unreliable_set)

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
