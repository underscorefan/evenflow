from typing import List, Set, Optional
from .functions import maintain_netloc, remove_prefix


class UrlSet:
    def __init__(self, initial_set: Optional[Set[str]] = None):
        if initial_set is None:
            initial_set = set()
        self.set = initial_set

    def contains(self, url: str, netloc: bool = True) -> bool:
        return self.__strip_if(url, netloc) in self.set

    def add(self, url: str, netloc: bool = True):
        self.set.add(self.__strip_if(url, netloc))

    def add_multiple(self, urls: List[str], netloc: bool = True):
        for url in urls:
            self.add(url, netloc=netloc)

    @staticmethod
    def __strip_if(url: str, netloc: bool) -> str:
        return remove_prefix(maintain_netloc(url) if netloc else url)
