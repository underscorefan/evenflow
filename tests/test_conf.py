from evenflow import Conf


def test_conf_sources():
    c = Conf(
        config_file='./data/fake_conf.json',
        backup_file_path='./data/backup.json',
        host_cache='',
        unreliable=''
    )

    name_set = {'snf', 'tof'}
    sources = c.load_sources()
    expected = 2

    one_expected = {
        "after": 93,
        "url": "https://fndet.com/arch/fake/page/8/"
    }

    assert sources is not None
    assert len([s.name for s in sources if s.name in name_set]) == expected
    assert len([s for s in sources if s.stop_after == one_expected["after"] and s.url == one_expected["url"]]) == 1
