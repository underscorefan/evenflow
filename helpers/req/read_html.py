from bs4 import BeautifulSoup
from aiohttp import ClientSession

__DECODER = "html5lib"


async def url_to_soup(url: str, session: ClientSession) -> BeautifulSoup:
    return soup_object(await fetch_html_get(url, session))


async def fetch_html_get(url: str, session: ClientSession) -> str:
    resp = await session.request(method="GET", url=url)
    return await resp.text()


def soup_object(response_text: str) -> BeautifulSoup:
    return BeautifulSoup(response_text, __DECODER)
