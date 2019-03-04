from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from helpers.func import mmap
from helpers.req import url_to_soup


class PageOps:
    def __init__(self, url: str, max_workers: int, **ops: Callable[[BeautifulSoup], any]):
        self.url = url
        self.ops = ops
        self.max_workers = max_workers

    def __do_ops_p(self, page: BeautifulSoup):
        executors = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for name, func in self.ops.items():
                executors.append(executor.submit(self.__func_wrapper, func, name, page))
        return dict([res.result() for res in executors])

    def __do_ops_s(self, page: BeautifulSoup):
        return dict([(name, func(page)) for name, func in self.ops.items()])

    async def do_ops(self, session: ClientSession):
        page = await url_to_soup(self.url, session)
        return self.__do_ops_s(page) if self.__not_par() else self.__do_ops_p(page)

    def fetch_multiple_urls(self, selector: str, key: str):
        self.add_op(key, lambda page: [a.get('href') for a in page.select(selector)])
        return self

    def fetch_single_url(self, selector: str, key: str):
        self.add_op(
            key,
            lambda page: mmap(page.select_one(selector), lambda tag: tag.get('href'))
        )
        return self

    def add_op(self, key: str, op: Callable[[BeautifulSoup], any]):
        self.ops[key] = op
        return self

    def __not_par(self):
        return self.max_workers == 1

    @staticmethod
    def __func_wrapper(func: Callable[[BeautifulSoup], any], key: str, page: BeautifulSoup):
        return key, func(page)