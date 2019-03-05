import socket
import asyncio
from functools import partial


def resolve_ip(loop: asyncio.events, host: str) -> asyncio.futures:
    return loop.run_in_executor(None, socket.gethostbyname, host)


def resolve_ip_future(loop: asyncio) -> asyncio.futures:
    return partial(loop.run_in_executor, None, socket.gethostbyname)
