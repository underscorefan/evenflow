from bs4 import BeautifulSoup
from aiohttp import ClientSession
from dirtyfunc import Either

__DECODER = "html5lib"


class HttpStatusError(Exception):
    """raised when http response status is not 200"""
    pass


async def __get_request(url: str, session: ClientSession) -> str:
    async with session.request(method="GET", url=url) as resp:
        if resp.status != 200:
            raise HttpStatusError(f'{url} responded with {resp.status}')
        return await resp.text()


async def new_soup(url: str, session: ClientSession) -> Either[Exception, BeautifulSoup]:
    attempt = await get_html(url, session)
    return attempt.map(lambda response_text: soup_from_response(response_text))


async def get_html(url: str, session: ClientSession) -> Either[Exception, str]:
    return await Either.attempt_awaitable(__get_request(url, session))


def soup_from_response(response_text: str) -> BeautifulSoup:
    return BeautifulSoup(response_text, __DECODER)
