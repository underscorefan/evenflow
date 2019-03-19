from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Any

from aiohttp import ClientSession
from bs4 import BeautifulSoup

from evenflow.helpers.req import url_to_soup
from dirtyfunc import Option


class PageOps:
    def __init__(self, url: str, max_workers: int, **ops: Callable[[BeautifulSoup], Any]):
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
            lambda page: Option(page.select_one(selector)).map(lambda tag: tag.get('href')).on_value()
        )
        return self

    def add_op(self, key: str, op: Callable[[BeautifulSoup], Any]):
        self.ops[key] = op
        return self

    def __not_par(self):
        return self.max_workers == 1

    @staticmethod
    def __func_wrapper(func: Callable[[BeautifulSoup], Any], key: str, page: BeautifulSoup):
        return key, func(page)
