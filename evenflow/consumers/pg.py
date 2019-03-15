import asyncpg
import asyncio

from typing import List
from evenflow.dbops import QueryManager
from evenflow.messages import ArticleExtended, Error
from evenflow.helpers import exc


async def store_articles(pool: asyncpg.pool.Pool, storage_queue: asyncio.Queue, error_queue: asyncio.Queue):
    query_builder = QueryManager(columns=ArticleExtended.columns(), table='article')
    while True:
        articles: List[ArticleExtended] = await storage_queue.get()
        print(f'received {len(articles)} articles')
        async with pool.acquire() as connection:
            stmt = await connection.prepare(query_builder.make_insert() + " RETURNING url")
            for article in articles:
                try:
                    value = await stmt.fetchval(*query_builder.sort_args(article.to_sql_dict()))
                    print(f"stored {value}")
                except Exception as e:
                    await error_queue.put(
                        Error(msg=exc.get_name(e), url=article.url_to_visit, source=article.scraped_from)
                    )

        storage_queue.task_done()


async def store_errors(pool: asyncpg.pool.Pool, error_queue: asyncio.Queue):
    query_builder = QueryManager(columns=Error.columns(), table='error')
    while True:
        error: Error = await error_queue.get()
        print(f'errors:\t{error.msg}')
        async with pool.acquire() as connection:
            try:
                await connection.execute(query_builder.make_insert(), *query_builder.sort_args(error.to_sql_dict()))
            except Exception as e:
                print(e)
        error_queue.task_done()
