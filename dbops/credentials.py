import asyncpg
from typing import Optional


class DatabaseCredentials:
    def __init__(self, user, host, password, name):
        self.user = user
        self.host = host
        self.password = password
        self.name = name
        self.credentials = {
            "database": name,
            "host": host,
            "user": user,
            "password": password
        }

    async def make_pool(self) -> Optional[asyncpg.pool.Pool]:
        try:
            return await asyncpg.create_pool(database=self.name, user=self.user, password=self.password, host=self.host)
        except asyncpg.exceptions.InvalidAuthorizationSpecificationError:
            return None

    async def connection(self) -> Optional[asyncpg.connection.Connection]:
        try:
            return await asyncpg.connect(**self.credentials)
        except Exception as e:
            print(e)
            return None
