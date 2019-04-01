import asyncpg
from typing import List


async def select_sources(conn: asyncpg.connection.Connection) -> List[asyncpg.Record]:
    return await conn.fetch("SELECT * FROM source;")


async def select_errors(conn: asyncpg.connection.Connection, fake: bool) -> List[asyncpg.Record]:
    return await conn.fetch(
        "SELECT *  FROM error  WHERE type != $1 AND type != $2 AND from_fake = $3",
        'UniqueViolationError',
        'HttpStatusError',
        fake
    )
