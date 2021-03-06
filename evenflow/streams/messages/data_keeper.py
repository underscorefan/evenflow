from typing import Dict, Tuple, ItemsView, List, Callable, Optional

from dirtyfunc import Either
from evenflow.streams.messages import Error, CollectorState


class DataKeeper:
    def __init__(
            self,
            initial_state: Optional[Dict[str, Dict]] = None,
            initial_links: Optional[Dict[str, Tuple[str, bool]]] = None
    ):
        self.__links_to_send: Dict[str, Tuple[str, bool]] = initial_links if initial_links is not None else {}
        self.__errors: List[Error] = []
        self.__state: Dict[str, Dict] = initial_state if initial_state is not None else {}

    def add_page_hrefs(
            self,
            scraped_from: str,
            fake: bool,
            maybe_hrefs: Either[Exception, Dict[str, Tuple[str, bool]]]
    ):
        if maybe_hrefs.empty:
            add_error = maybe_hrefs.on_left(lambda e: Error.from_exception(exc=e, url=scraped_from, fake=fake))
            self.__errors.append(add_error)
            return

        self.append_links(maybe_hrefs.on_right())

    def append_link(self, url: str, source: str, mark_as_fake: bool):
        self.__links_to_send[url] = (source, mark_as_fake)

    def append_links(self, additional_links: Dict[str, Tuple[str, bool]]):
        self.__links_to_send = {**self.__links_to_send, **additional_links}

    def append_errors(self, additional_errors: List[Error]):
        self.__errors = self.__errors + additional_errors

    def filter(self, func: Callable[[str, Tuple[str, bool]], bool]) -> 'DataKeeper':
        ret = DataKeeper(initial_state=self.state)
        ret.append_links({k: v for k, v in self.__links_to_send.items() if func(k, v)})
        ret.append_errors(self.errors)
        return ret

    def append_state(self, state: CollectorState) -> 'DataKeeper':
        name, data = state.unpack()
        self.__state[name] = data
        return self

    def append_states(self, states: List[CollectorState]) -> 'DataKeeper':
        for state in states:
            name, data = state.unpack()
            self.__state[name] = data
        return self

    def __add__(self, other: 'DataKeeper') -> 'DataKeeper':
        ret = DataKeeper(initial_state={**self.state, **other.state}, initial_links=self.links_to_send)
        ret.append_links(other.links_to_send)
        ret.append_errors(other.errors + self.errors)
        return ret

    @property
    def items(self) -> ItemsView[str, Tuple[str, bool]]:
        return self.__links_to_send.items()

    @property
    def links_to_send(self) -> Dict[str, Tuple[str, bool]]:
        return self.__links_to_send

    @property
    def errors(self) -> List[Error]:
        return self.__errors

    @property
    def state(self) -> Dict[str, Dict]:
        return self.__state
