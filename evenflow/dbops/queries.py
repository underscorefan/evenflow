from asyncpg.connection import Connection
from asyncpg import Record

from typing import List


async def select_sources(conn: Connection) -> List[Record]:
    return await conn.fetch("SELECT * FROM source;")


async def select_errors(conn: Connection, fake: bool) -> List[Record]:
    return await conn.fetch(
        "SELECT *  FROM error  WHERE type = $1 OR type LIKE $2 AND from_fake = $3",
        'TimeoutError',
        'Client%',
        fake
    )


async def delete_errors(conn: Connection):
    dupl = await conn.fetch("""
        DELETE 
            FROM error a 
            USING error b
        WHERE a.creation_date < b.creation_date
        AND a.url = b.url;
    """)
    res = await conn.fetch("""
        DELETE 
            FROM error e 
            USING article a
        WHERE e.url = a.url
        OR e.url = a.visited_url;
        """)
    return dupl, res
