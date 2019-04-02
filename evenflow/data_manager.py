from functools import partial
from typing import Optional

from asyncpg.pool import Pool

from evenflow.dbops import DatabaseCredentials, queries
from evenflow.streams import consumers
from evenflow.urlman import LabelledSources

HIGH, LOW, MIXED, ARCHIVE = "high", "low", "mixed", "archive"


class DataManager:
    def __init__(self, db_credentials: DatabaseCredentials):
        self.db_credentials = db_credentials
        self.__pool: Optional[Pool] = None

    @property
    async def article_rules(self) -> consumers.ArticleRules:
        labelled_sources = await self.make_url_dict()
        add_url_partial = partial(self.add_url, labelled_sources)
        return consumers.ArticleRules(lambda url, from_fake, archived: add_url_partial(url, from_fake, archived))

    async def make_url_dict(self) -> LabelledSources:
        records = await self.db_credentials.do_with_connection(lambda c: queries.select_sources(c))
        labelled_sources = LabelledSources(False)
        true_labels, fake_labels = {'very high', 'high'}, {'low', 'insane stuff', 'satire', 'very low'}

        for record in records:
            label, url = record['factual_reporting'], record["url"]

            if label == MIXED:
                labelled_sources[url] = MIXED
                continue

            if label in true_labels:
                labelled_sources[url] = HIGH
                continue

            if label in fake_labels:
                labelled_sources[url] = LOW

        for arch in [f"archive.{dom}" for dom in ["is", "fo", "today"]] + ["web.archive.org"]:
            labelled_sources[arch] = ARCHIVE

        return labelled_sources.strip(True)

    @property
    async def pool(self) -> Pool:
        if self.__pool is None:
            print("about to create pool")
            self.__pool = await self.db_credentials.make_pool()
        return self.__pool

    async def close_pool(self):
        if self.__pool is not None:
            await self.__pool.close()

    async def delete_errors(self):
        await self.db_credentials.do_with_connection(lambda conn: queries.delete_errors(conn))

    @staticmethod
    def add_url(labelled_sources: LabelledSources, url: str, from_fake: bool, from_archive: bool) -> bool:
        label = labelled_sources[url]

        if label == HIGH:
            return not from_fake

        if label == LOW:
            return from_fake

        if label == MIXED:
            return from_fake and from_archive

        return label == ARCHIVE and from_fake and not from_archive