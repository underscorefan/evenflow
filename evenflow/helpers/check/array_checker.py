from typing import List, Callable, Any


def check_list(lst: List, condition: Callable[[Any, ], bool], apply: Callable[[Any, ], Any]) -> List:
    return [apply(element) for element in lst if condition(element)]


def check_strings(l: List) -> List[str]:
    return check_list(
        lst=l,
        condition=lambda el: isinstance(el, str) and el.strip() != '',
        apply=lambda el: el.strip()
    )
