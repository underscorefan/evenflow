from bs4 import BeautifulSoup
from aiohttp import ClientSession
from dirtyfunc import Either


__DECODER = "html5lib"


async def __real_request(url: str, session: ClientSession) -> str:
    resp = await session.request(method="GET", url=url)
    return await resp.text()


async def url_to_soup(url: str, session: ClientSession) -> Either[Exception, BeautifulSoup]:
    attempt = await fetch_html_get(url, session)
    return attempt.map(lambda response_text: soup_object(response_text))
    # return soup_object(await fetch_html_get(url, session))


async def fetch_html_get(url: str, session: ClientSession) -> Either[Exception, str]:
    return await Either.attempt_awaitable(__real_request(url, session))


def soup_object(response_text: str) -> BeautifulSoup:
    return BeautifulSoup(response_text, __DECODER)
