from typing import Dict, Tuple

OVER, NAME, DATA = 'is_over', 'name', 'data'


class State:
    def __init__(self, name: str, is_over: bool, data: Dict):
        self.name = name
        self.is_over = is_over
        self.data = data

    def unpack(self) -> Tuple[str, Dict]:
        return self.name, {OVER: self.is_over, DATA: self.data}

    @staticmethod
    def pack(name: str, d: Dict) -> 'State':
        return State(**{**{NAME: name}, **d})
