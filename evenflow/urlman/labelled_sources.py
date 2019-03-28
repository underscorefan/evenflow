from typing import List, Optional, Dict

from .functions import maintain_netloc, remove_prefix


class LabelledSources:
    def __init__(self, strip_path: bool):
        self.__dict: Dict[str, str] = {}
        self.__strip = strip_path

    def __setitem__(self, url: str, label: str):
        self.__dict[self.__strip_if(url)] = label

    def __contains__(self, url: str) -> bool:
        return self.__strip_if(url) in self.__dict

    def __getitem__(self, url: str) -> Optional[str]:
        return self.__dict.get(self.__strip_if(url))

    def strip(self, strip_path: bool) -> 'LabelledSources':
        self.__strip = strip_path
        return self

    def __strip_if(self, url: str) -> str:
        return remove_prefix(maintain_netloc(url) if self.__strip else url)

    def keys(self) -> List[str]:
        return [k for k in self.__dict]
