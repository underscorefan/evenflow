from evenflow import Conf


def test_conf_sources():
    c = Conf(config_file='./data/fake_conf.json', backup_file_path='', host_cache='', unreliable='')
    name_set = {'snf', 'tof'}
    sources = c.load_sources()
    expected = 2
    assert sources is not None
    assert len([s.name for s in sources if s.name in name_set]) == expected
