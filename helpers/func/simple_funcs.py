from typing import Callable


def mmap(value: any, func:  Callable[[any], any]):
    return func(value) if value is not None else None
