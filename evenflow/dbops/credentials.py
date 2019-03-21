import asyncpg
from typing import Optional, Callable, TypeVar, Awaitable


class DatabaseCredentials:
    T = TypeVar('T')

    def __init__(self, user, host, password, name):
        self.credentials = {
            "database": name,
            "host": host,
            "user": user,
            "password": password
        }

    async def make_pool(self) -> Optional[asyncpg.pool.Pool]:
        return await self.__args_with(asyncpg.create_pool)

    async def connection(self) -> Optional[asyncpg.connection.Connection]:
        return await self.__args_with(asyncpg.connect)

    async def do_with_connection(self, action: Callable[[asyncpg.connection.Connection], Awaitable[T]]) -> T:
        conn = await self.connection()
        if conn:
            ret = await action(conn)
            await conn.close()
            return ret
        return None

    async def __args_with(self, f: Callable) -> Optional:
        try:
            return await f(**self.credentials)
        except asyncpg.exceptions.InvalidAuthorizationSpecificationError:
            return None
