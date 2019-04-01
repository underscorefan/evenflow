from praw import Reddit
from asyncio import Queue, events
from typing import Dict, Optional
from evenflow.streams.messages import DataKeeper, CollectorState


class RedditSettings:
    def __init__(self, subreddits: Dict[str, bool], num_posts: int, instance: Reddit):
        self.subreddits = subreddits
        self.num_posts = num_posts
        self.instance = instance

    def sub(self, sub: str):
        return self.instance.subreddit(sub)

    def sub_is_fake(self, sub: str) -> Optional[bool]:
        return self.subreddits.get(sub)


def add_domain(subreddit: str) -> str:
    return f"https://www.reddit.com/r/{subreddit}/"


async def collect_links_reddit(rsm: RedditSettings, send_channel: Queue):
    for subreddit in rsm.subreddits:
        edk = DataKeeper()
        tot = 0
        for submission in rsm.sub(subreddit).top(time_filter='year', limit=rsm.num_posts):
            subreddit_domain = add_domain(subreddit)
            if submission.url != submission.permalink:
                edk.append_link(submission.url, subreddit_domain, rsm.sub_is_fake(subreddit))
                tot += 1

        state = CollectorState(name=subreddit, is_over=True, data={'posts': tot})
        await send_channel.put(edk.append_state(state))
