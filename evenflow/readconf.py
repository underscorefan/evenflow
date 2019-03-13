import asyncio
import argparse
import os

from typing import List, Optional, Dict, ItemsView
from functools import partial
from evenflow.helpers.hostracker import HostTracker
from evenflow.helpers.unreliableset import UnreliableSet
from evenflow.helpers.file import read_json_from
from evenflow.dbops import DatabaseCredentials
from evenflow.fci import State, FeedReader, FeedReaderHTML
from evenflow.pkginfo import short_description


class Conf:
    def __init__(self, host_cache: str, unreliable: str, backup_file_path: str, config_file: str):
        config_data = read_json_from(config_file)
        self.host_cache = host_cache
        self.unreliable = unreliable
        self.backup_file_path = backup_file_path
        self.sources_json = config_data.get("sources")
        self.pg_cred = config_data.get("pg_cred")

    def load_host_tracker(self, loop: asyncio.events) -> HostTracker:
        to_ret = HostTracker(loop=loop)
        try:
            to_ret.load_from_json(path=self.host_cache)
        finally:
            return to_ret

    def load_sources(self) -> Optional[List[FeedReader]]:
        bkp = self.__load_backup()
        f = partial(self.__make_reader, bkp_obj=bkp)

        try:
            return list(
                filter(
                    lambda x: x,
                    [f(s.items()) for s in self.sources_json]
                )
            )

        except KeyError:
            return None

    @staticmethod
    def __make_reader(json_data: ItemsView, bkp_obj: Optional[Dict[str, Dict]]) -> Optional[FeedReader]:
        reader = FeedReaderHTML(**{k: v for k, v in json_data if k != "type"})
        if not bkp_obj:
            return reader

        old_state_obj = bkp_obj.get(reader.get_name())
        if old_state_obj:
            recovered = reader.recover_state(State.pack(reader.get_name(), old_state_obj))
            if not recovered:
                return None

        return reader

    def load_unreliable(self) -> UnreliableSet:
        return UnreliableSet(initial_set=set(read_json_from(self.unreliable)))

    def __load_backup(self) -> Optional[Dict[str, Dict]]:
        try:
            return read_json_from(self.backup_file_path)
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
