import asyncpg
from typing import List


async def select_sources(conn: asyncpg.connection.Connection) -> List[asyncpg.Record]:
    return await conn.fetch("SELECT * FROM source;")
