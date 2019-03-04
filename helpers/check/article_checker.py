from typing import Optional
from messages import ArticleExtended


class ArticleChecker:
    def __init__(self):
        self.duplicate_title = set()
        self.duplicate_url = set()

    def is_valid(self, a: Optional[ArticleExtended]) -> bool:

        if a is None:
            return False

        title = a.title.strip()
        url = a.actual_url.strip()

        if title in self.duplicate_title:
            return False

        if url in self.duplicate_url:
            return False

        self.duplicate_url.add(url)
        self.duplicate_title.add(title)

        return True

    def flush_dicts(self):
        self.duplicate_title = set()
        self.duplicate_url = set()
