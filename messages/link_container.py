from typing import Dict, Tuple, ItemsView


class LinkContainer:
    def __init__(self, links: Dict[str, Tuple[str, bool]], backup: dict):
        self.links = links
        self.backup = backup

    def __str__(self):
        to_print = ''
        for link, source in self.links:
            to_print += f'link: {link}\t from: {source}'
        return to_print + f'backup:\n\t{self.backup}\n'

    def items(self) -> ItemsView[str, Tuple[str, bool]]:
        return self.links.items()
