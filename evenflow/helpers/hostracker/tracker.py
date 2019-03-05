from evenflow.helpers.req import resolve_ip_future
from typing import Optional
from evenflow.helpers.req import maintain_netloc
import asyncio
import socket
import json


class HostTracker:
    def __init__(self, loop: asyncio.events):
        self.__tracker_dict = {}
        self.__retrieve_ip_from_host = resolve_ip_future(loop)

    def load_from_json(self, path):
        with open(path) as f:
            self.__tracker_dict = json.load(f)

    async def get_ip(self, url: str, parse: bool = True) -> Optional[str]:
        parsed_url = maintain_netloc(url) if parse else url
        if parsed_url in self.__tracker_dict:
            return self.__tracker_dict[parsed_url]
        if len(parsed_url.strip()) == 0:
            return None
        try:
            ret = await self.__retrieve_ip_from_host(parsed_url)
            self.__tracker_dict[parsed_url] = ret
            return ret
        except socket.error:
            return None

    def dict_to_json(self) -> str:
        return json.dumps(self.__tracker_dict)

    def store_dict(self, path):
        with open(path, mode='w') as f:
            json.dump(self.__tracker_dict, f)
