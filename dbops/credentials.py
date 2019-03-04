import asyncpg
from typing import Optional, Callable


class DatabaseCredentials:
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

    async def __args_with(self, f: Callable) -> Optional:
        try:
            return await f(**self.credentials)
        except asyncpg.exceptions.InvalidAuthorizationSpecificationError:
            return None
