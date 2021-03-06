import argparse

from evenflow import short_description, Conf
from evenflow.bootstrap import run


def read_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=short_description)
    parser.add_argument('-c', '--conf', help="specify config file location", required=True, type=str)
    parser.add_argument(
        '-r', '--restore', help="restore errors stored in database", action='store_true', required=False
    )
    return parser.parse_args()


def conf_from_cli() -> Conf:
    cli = read_cli_args()
    return Conf(cli.conf, cli.restore)


if __name__ == '__main__':
    run(conf_from_cli())
