import re
from typing import List, Optional


def check_strings(lst: List) -> List[str]:
    ret: List[str] = []
    for element in lst:
        if isinstance(element, str):
            element = element.strip()
            if element != '':
                ret.append(element)
    return ret


def value_or_none(to_check) -> Optional[str]:
    if isinstance(to_check, str):
        comodo = to_check.strip()
        return comodo if comodo != '' else None
    return None


def remove_newlines(text: str) -> str:
    return re.sub(r'[\n]+', ' ', text).strip()


def min_words_or_none(text: str, min_words: int) -> Optional[str]:
    stripped = text.strip()
    return stripped if len(stripped.split(" ")) >= min_words else None