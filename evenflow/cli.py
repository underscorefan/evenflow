from evenflow.bootstrap import run
from evenflow.read_conf import conf_from_cli


if __name__ == '__main__':
    run(conf_from_cli())
