from typing import Callable, Any


def mmap(value: Any, func:  Callable[[Any], Any]):
    return func(value) if value is not None else None
