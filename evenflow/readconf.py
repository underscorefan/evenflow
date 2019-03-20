import argparse
import os
import io
import json
from typing import List, Optional, Dict, ItemsView
from evenflow.urlman import UrlSet
from evenflow.dbops import DatabaseCredentials
from evenflow.scrapers.feed import FeedScraperState, FeedScraper, SiteFeed
from evenflow.pkginfo import short_description


def read_json_from(path: str):
    with io.open(path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)


class Conf:
    def __init__(self, unreliable: str, backup_file_path: str, config_file: str):
        config_data = read_json_from(config_file)
        self.unreliable = unreliable
        self.backup_file_path = backup_file_path
        self.initial_state = self.__load_backup(backup_file_path)
        self.sources_json = config_data.get("sources")
        self.pg_cred = config_data.get("pg_cred")

    def load_sources(self) -> Optional[List[FeedScraper]]:
        try:
            return list(
                filter(
                    lambda x: x,
                    [self.__new_reader(s.items()) for s in self.sources_json if s["type"] == "html"]
                )
            )

        except KeyError:
            return None

    def __new_reader(self, json_data: ItemsView) -> Optional[FeedScraper]:
        reader = SiteFeed(**{k: v for k, v in json_data if k != "type"})
        if not self.initial_state:
            return reader

        old_state_obj = self.initial_state.get(reader.get_name())
        if old_state_obj:
            recovered = reader.recover_state(FeedScraperState.pack(reader.get_name(), old_state_obj))
            if not recovered:
                return None

        return reader

    def load_unreliable(self) -> UrlSet:
        return UrlSet(initial_set=set(read_json_from(self.unreliable)))

    @staticmethod
    def __load_backup(path) -> Optional[Dict[str, Dict]]:
        try:
            return read_json_from(path)
        except FileNotFoundError:
            return None

    def setupdb(self) -> Optional[DatabaseCredentials]:
        try:
            return DatabaseCredentials(
                user=self.pg_cred["user"],
                password=self.pg_cred["pwd"],
                name=self.pg_cred["db"],
                host=self.pg_cred["host"]
            )
        except KeyError:
            return None


def read_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=short_description)
    parser.add_argument('-u', '--unreliable', help="specify unreliable hosts json file", required=True, type=str)
    parser.add_argument('-b', '--backup', help="specify where to save back-up", required=True, type=str)
    parser.add_argument('-c', '--conf', help="specify config file location", required=True, type=str)
    parser.add_argument('-p', '--path', help="base path for files", type=str, default=None)
    return parser.parse_args()


def __make_dict(cli: argparse.Namespace) -> Dict[str, str]:
    return {
        'backup_file_path': cli.backup,
        'config_file': cli.conf,
        'unreliable': cli.unreliable
    }


def conf_from_cli() -> Conf:
    cli = read_cli_args()
    cli_dict = __make_dict(cli)
    if cli.path is not None:
        cli_dict = {arg: os.path.join(cli.path, value) for arg, value in cli_dict.items()}
    return Conf(**cli_dict)
