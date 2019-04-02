from newspaper import Article

from evenflow import Conf
from evenflow.scrapers.feed.site_feed import PAGE, URL
from evenflow.streams.consumers import ArticleRules
from evenflow.streams.messages import ArticleExtended


def base_conf(config_file: str):
    return Conf(config_file=config_file, restore=False)


def test_conf_sources():
    c = base_conf('./data/fake_conf.json')

    name_set = {'snf', 'tof'}
    sources = c.load_sources()
    expected = 2

    assert sources is not None
    assert len([s.get_name() for s in sources.on_right() if s.get_name() in name_set]) == expected


def test_conf_backup():
    c = base_conf('./data/fake_conf.json')

    expected_state_data = {
        "after": 93,
        "url": "https://fndet.com/arch/fake/page/8/"
    }

    total, expected_total = 0, 1

    for s in c.load_sources().on_right():
        state = s.to_state()
        if state.data[URL] == expected_state_data["url"] and state.data[PAGE] == expected_state_data["after"]:
            total += 1

    assert total == expected_total


def test_conf_over():
    c = base_conf('./data/fake_conf_over.json')
    assert len(c.load_sources().on_right()) == 1


def test_conf_rules():
    ar = ArticleRules(lambda url, from_fake: True)

    c = base_conf('./data/fake_conf.json')
    ar = c.load_rules_into(ar)

    pass_it = "https://www.theguardian.com/us-news/2019/mar/25/california-water-drought-scarce-saudi-arabia"
    not_pass_it = "http://www.dirittierovesci.com/team"

    assert ar.pass_checks(fake_extended(pass_it)) is True
    assert ar.pass_checks(fake_extended(not_pass_it)) is False


def test_conf_check_feed():
    c = base_conf('./data/fake_conf_condition.json')
    sources = c.load_sources()
    found = False
    for source in sources.on_right():
        if source.get_name() == "tof":
            found = True
            assert source.condition.value_container == ".try"
            assert source.condition.satisfied("megafAke") is True
            assert source.condition.satisfied("megafA") is False
    assert found is True


def fake_extended(url: str) -> ArticleExtended:
    a = Article(url)
    a.download()
    return ArticleExtended(html=a.html, url_to_visit=url, scraped_from="nope", fake=False)
