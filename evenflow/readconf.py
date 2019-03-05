import asyncio
import argparse
import os
from typing import List, Optional, Dict
from evenflow.helpers.hostracker import HostTracker
from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.helpers.file import read_json_from
from evenflow.dbops import DatabaseCredentials
from evenflow.fci import SourceSpider


class Conf:
    def __init__(self, host_cache: str, unreliable: str, backup_file_path: str, config_file: str):
        self.host_cache = host_cache
        self.unreliable = unreliable
        self.backup_file_path = backup_file_path
        self.sources = [
            SourceSpider(
                name="snopes",
                page_to_scrape="https://www.snopes.com/fact-check/rating/false/",
                next_page="div.pagination > a.btn-next",
                articles="div.list-group > article.list-group-item > a",
                text_anchors=".post-body-card a",
                num_pages=1,
                is_fake=True
            ),
            SourceSpider(
                name="truth_or_fiction",
                page_to_scrape="https://www.truthorfiction.com/category/fact-checks/disinformation/",
                next_page="a.next",
                articles="#content-wrapper > div.container > div.row > div > div.tt-post > div > a.tt-post-title.c-h5",
                text_anchors="div.simple-text a",
                num_pages=1,
                is_fake=True
            ),
        ]
        self.pg_cred = read_json_from(config_file).get("pg_cred")

    def load_host_tracker(self, loop: asyncio.events) -> HostTracker:
        to_ret = HostTracker(loop=loop)
        try:
            to_ret.load_from_json(path=self.host_cache)
        finally:
            return to_ret

    def get_sources(self) -> List[SourceSpider]:
        return self.sources

    def load_unreliable(self) -> UnreliableSet:
        return UnreliableSet(initial_set=set(read_json_from(self.unreliable)))

    def setupdb(self) -> Optional[DatabaseCredentials]:
        try:
            pg = self.pg_cred
            return DatabaseCredentials(user=pg["user"], password=pg["pwd"], name=pg["db"], host=pg["host"])
        except KeyError:
            return None


def read_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="fetching articles from the web")
    parser.add_argument('-t', '--tracker', help="specify host ip json file", required=True, type=str)
    parser.add_argument('-u', '--unreliable', help="specify unreliable hosts json file", required=True, type=str)
    parser.add_argument('-b', '--backup', help="specify where to save back-up", required=True, type=str)
    parser.add_argument('-c', '--conf', help="specify config file location", required=True, type=str)
    parser.add_argument('-p', '--path', help="base path for files", type=str, default=None)
    return parser.parse_args()


def __make_dict(cli: argparse.Namespace) -> Dict[str, str]:
    return {
        'backup_file_path': cli.backup,
        'config_file': cli.conf,
        'host_cache': cli.tracker,
        'unreliable': cli.unreliable
    }


def conf_from_cli() -> Conf:
    cli = read_cli_args()
    cli_dict = __make_dict(cli)
    if cli.path is not None:
        cli_dict = {arg: os.path.join(cli.path, value) for arg, value in cli_dict.items()}
    return Conf(**cli_dict)
