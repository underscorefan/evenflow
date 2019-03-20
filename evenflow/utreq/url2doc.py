from bs4 import BeautifulSoup
from aiohttp import ClientSession
from dirtyfunc import Either


__DECODER = "html5lib"


async def __get_request(url: str, session: ClientSession) -> str:
    resp = await session.request(method="GET", url=url)
    return await resp.text()


async def new_soup(url: str, session: ClientSession) -> Either[Exception, BeautifulSoup]:
    attempt = await get_html(url, session)
    return attempt.map(lambda response_text: soup_from_response(response_text))


async def get_html(url: str, session: ClientSession) -> Either[Exception, str]:
    return await Either.attempt_awaitable(__get_request(url, session))


def soup_from_response(response_text: str) -> BeautifulSoup:
    return BeautifulSoup(response_text, __DECODER)
