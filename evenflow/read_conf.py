import io
import json
import praw

from functools import partial
from typing import List, Optional, Dict, ItemsView
from dirtyfunc import Either, Left, Right
from evenflow.dbops import DatabaseCredentials
from evenflow.scrapers.feed import FeedScraper, SiteFeed
from evenflow.streams.messages import CollectorState
from evenflow.streams.consumers import ArticleRules
from evenflow.streams.producers import RedditSettings


def read_json_from(path: str):
    with io.open(path, 'r', encoding='utf-8-sig') as f:
        return json.load(f)


class Conf:
    def __init__(self, config_file: str, restore: bool):
        config_data = read_json_from(config_file)
        self.restore = restore
        self.backup_file_path = config_data.get("backup")
        self.initial_state = self.__load_backup(self.backup_file_path)
        self.sources_json = config_data.get("sources")
        self.pg_cred = config_data.get("pg_cred")
        self.rules: Dict = config_data.get("rules")
        self.reddit: Dict = config_data.get("reddit")

    def load_sources(self) -> Either[Exception, List[FeedScraper]]:
        try:
            scrapers = list(filter(lambda x: x, [self.__new_reader(s.items()) for s in self.sources_json]))
            if len(scrapers) == 0:
                return Left[Exception](ValueError("No scrapers found"))

            return Right[List[FeedScraper]](scrapers)

        except Exception as e:
            return Left[Exception](e)

    def __load_reddit(self) -> RedditSettings:
        subs = self.reddit.get("subs")
        if subs is None:
            raise ValueError("subs is not defined")

        num_posts = int(self.reddit.get("num_posts"))
        if num_posts is None:
            raise ValueError("num_posts is not defined")

        credentials = self.reddit.get("credentials")
        if credentials is None:
            raise ValueError("credentials is not defined")

        subreddits = {k: v for k, v in subs.items() if not self.__subreddit_over(k)}
        if len(subreddits) == 0:
            raise ValueError("No subreddits found")

        return RedditSettings(
            subreddits=subreddits,
            num_posts=num_posts,
            instance=praw.Reddit(**credentials)
        )

    def __subreddit_over(self, subreddit: str) -> bool:
        if self.initial_state is None:
            return False

        subreddit_state = self.initial_state.get(subreddit)
        if subreddit_state is not None:
            return CollectorState.pack(subreddit, subreddit_state).is_over

        return False

    def load_reddit_settings(self) -> Either[Exception, RedditSettings]:
        return Either.attempt(self.__load_reddit)

    def __new_reader(self, json_data: ItemsView) -> Optional[FeedScraper]:
        reader = SiteFeed(**{k: v for k, v in json_data if k != "type"})
        if not self.initial_state:
            return reader

        old_state_obj = self.initial_state.get(reader.get_name())
        if old_state_obj:
            recovered = reader.recover_state(CollectorState.pack(reader.get_name(), old_state_obj))
            if not recovered:
                return None

        return reader

    def load_rules_into(self, article_rules: ArticleRules) -> ArticleRules:
        for key, value in self.rules.items():
            if key == "blacklist":
                self.__add_sets_to(article_rules, value)
            elif key == "min_length":
                self.__add_min_lengths(article_rules, value)
        return article_rules

    @staticmethod
    def __add_sets_to(article_rules: ArticleRules, dict_sets: Dict):
        for key, values in dict_sets.items():
            if key == "titles":
                article_rules.add_title_check(lambda title: title not in set(values))
            elif key == "urls":
                article_rules.add_url_check(lambda url: url not in set(values))

    @staticmethod
    def __add_min_lengths(article_rules: ArticleRules, dict_ml: Dict[str, int]):
        def more_than(min_len: int, ch: str) -> bool:
            return len(ch) >= min_len

        for key, value in dict_ml.items():
            if key == "title":
                j = int(value)
                article_rules.add_title_check(lambda t: more_than(j, t))
            elif key == "text":
                x = int(value)
                article_rules.add_text_check(lambda t: more_than(x, t))
            elif key == "path_loc":
                z = int(value)
                article_rules.add_path_check(lambda p: more_than(z, p))

    @staticmethod
    def __load_backup(path: str) -> Optional[Dict[str, Dict]]:
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
