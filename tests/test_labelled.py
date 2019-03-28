from evenflow.urlman import LabelledSources


def test_labelled():
    urls = LabelledSources(strip_path=True)
    urls["https://www.arcticfoxnews.com/politics/polar-bear-wants-to-build-wall-along-canada-border"] = "mega"
    assert urls["https://www.arcticfoxnews.com/politics"] == "mega"
    assert ("arcticfoxnews.com" in urls.strip(strip_path=False)) is True
