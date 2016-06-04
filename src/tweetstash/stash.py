from abc import ABCMeta, abstractmethod
from pathlib import Path
import json


class Stash(metaclass=ABCMeta):
    """Abstract base class for tweetstash backends."""
    @abstractmethod
    def is_stashed(self, tweet_id):
        pass

    @abstractmethod
    def stash(self, tweet):
        pass

    def stash_many(self, tweets):
        for tweet in tweets:
            self.stash(tweet)

    @abstractmethod
    def unstash(self, tweet_id):
        pass

    def unstash_many(self, tweet_ids):
        for tweet_id in tweet_ids:
            yield self.unstash(tweet_id)

    def unstash_all(self):
        yield from self.unstash_many(self.all_ids())

    @abstractmethod
    def all_ids(self):
        pass


class FileStash(Stash):
    """"Filesystem tweetstash backend.
    Tweets are stored as separate JSON files, with the id as filename.

    """
    def __init__(self, base_dir=None):
        self.base_dir = Path('.') if base_dir is None else Path(base_dir)

    def tweet_path(self, tweet_id):
        return self.base_dir / (tweet_id + '.json')

    def is_stashed(self, tweet_id):
        return self.tweet_path(tweet_id).exists()

    def stash(self, tweet, overwrite=False):
        tweet_id = tweet['id_str']
        if overwrite or not self.is_stashed(tweet_id):
            with self.tweet_path(tweet_id).open('w') as file:
                json.dump(tweet, file)

    def unstash(self, tweet_id):
        with self.tweet_path(tweet_id).open() as file:
            return json.load(file)

    def all_ids(self):
        for tweet_path in self.base_dir.glob('*.json'):
            yield tweet_path.stem
