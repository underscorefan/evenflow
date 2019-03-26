from bs4 import BeautifulSoup
from aiohttp import ClientSession
from typing import Optional
from dirtyfunc import Either, Left, Right
from functools import partial
import asyncio

__DECODER = "html5lib"


class HttpStatusError(Exception):
    """raised when http response status is not 200"""
    pass


class TextError(Exception):
    """raised when text is not a string"""
    pass


async def __get_request(url: str, session: ClientSession) -> Either[Exception, str]:
    async with session.request(method="GET", url=url) as resp:
        if resp.status != 200:
            return Left(HttpStatusError(f'{url} responded with {resp.status}'))

        text = await resp.text()
        if text is None:
            return Left(TextError(f"{url} didn't respond with a string"))

        return Right(text)


async def new_soup(url: str, session: ClientSession) -> Either[Exception, BeautifulSoup]:
    attempt = await get_html(url, session)
    return attempt.map(lambda response_text: soup_from_response(response_text))


async def get_html(url: str, session: ClientSession, timeout: Optional[int] = None) -> Either[Exception, str]:
    call = partial(__get_request, url, session)
    try:
        coro = asyncio.wait_for(call(), timeout=timeout) if timeout else call()
        return await coro
    except Exception as e:
        return Left(e)


def soup_from_response(response_text: str) -> BeautifulSoup:
    return BeautifulSoup(response_text, __DECODER)
