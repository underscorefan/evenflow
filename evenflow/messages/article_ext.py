from typing import Optional, Dict, List
from bs4 import BeautifulSoup
from newspaper import Article
from newspaper.configuration import Configuration
from evenflow.helpers.check.string_checker import min_words_or_none, value_or_none, remove_newlines
from evenflow.helpers.check.array_checker import check_strings
from evenflow.helpers.req import soup_object, urlman
from .storable import Storable


class ArticleExtended(Article, Storable):

    def __init__(self, html: str, url_to_visit: str, scraped_from: str, fake: bool,
                 conf: Optional[Configuration] = None, do_nlp: bool = True):

        super().__init__(url='', config=conf if conf is not None else Configuration())
        super().set_html(html)
        super().parse()
        if do_nlp:
            super().nlp()
        self.fake = fake
        self.url_to_visit: str = url_to_visit
        self.scraped_from: str = scraped_from
        self.soup: BeautifulSoup = soup_object(html)
        self.actual_url: str = url_to_visit
        self.__text_length: Optional[int] = None

    def correct_title(self):
        if self.title.strip().endswith("…"):
            title_cmd = self.soup.select_one("title").text.strip()
            index = self.__find_last(title_cmd)
            self.set_title(title_cmd[:index] if index is not None else title_cmd)

    def to_sql_dict(self) -> Dict[str, str]:
        return {
            'title': self.title,
            'text': self.text,
            'description': min_words_or_none(self.meta_description, 1),
            'url': self.actual_url,
            'visited_url': self.url_to_visit,
            'scraped_from': self.scraped_from,
            'netloc': urlman.strip(self.actual_url),
            'path': urlman.maintain_path(self.actual_url),
            'authors': check_strings(self.authors),
            'images': check_strings(self.images),
            'videos': check_strings(self.movies),
            'lang': self.meta_lang,
            'keywords': check_strings(self.meta_keywords),
            'section': self.meta_data['article'].get('section'),
            'publish_date': self.publish_date,
            'generator': value_or_none(self.meta_data['generator']),
            'summary': value_or_none(self.summary),
            'fake': self.fake
        }

    @staticmethod
    def columns() -> List[str]:
        return article_sql_fields()

    def set_actual_url(self, url: str):
        self.actual_url = url

    def text_length(self) -> int:
        if self.__text_length is None:
            self.__text_length = len(self.text)
        return self.__text_length

    def remove_newlines_from_fields(self):
        self.set_text(remove_newlines(self.text))
        self.set_summary(remove_newlines(self.summary))

    @staticmethod
    def __find_last(title: str) -> Optional[int]:
        for find in [" - ", " | ", " – "]:
            maybe_index = title.rfind(find)
            if maybe_index > -1:
                return maybe_index
        return None


def article_sql_fields():
    return list(ArticleExtended('<h1></h1>', '', '', do_nlp=False, fake=False).to_sql_dict().keys())
