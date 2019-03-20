from evenflow import Conf
from evenflow.scrapers.feed.sitefeed import PAGE, URL
from functools import partial


def add_backup_to(config_file: str):
    return partial(Conf, config_file=config_file, host_cache='', unreliable='')


base_fake_conf = add_backup_to(config_file='./data/fake_conf.json')


def test_conf_sources():
    c = base_fake_conf(backup_file_path='')

    name_set = {'snf', 'tof'}
    sources = c.load_sources()
    expected = 2

    assert sources is not None
    assert len([s.get_name() for s in sources if s.get_name() in name_set]) == expected


def test_conf_backup():
    c = base_fake_conf(backup_file_path='./data/backup.json')

    expected_state_data = {
        "after": 93,
        "url": "https://fndet.com/arch/fake/page/8/"
    }

    total, expected_total = 0, 1

    for s in c.load_sources():
        state = s.to_state()
        if state.data[URL] == expected_state_data["url"] and state.data[PAGE] == expected_state_data["after"]:
            total += 1

    assert total == expected_total


def test_conf_over():
    c = base_fake_conf(backup_file_path='./data/backup_over.json')
    assert len(c.load_sources()) == 1

