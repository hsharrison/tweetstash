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
    def __init__(self, base_dir=None, create_dir=False, by_user=True):
        self.by_user = by_user
        self.base_dir = Path('.') if base_dir is None else Path(base_dir)
        if create_dir:
            self.base_dir.mkdir(parents=True, exist_ok=True)
        if not self.base_dir.exists():
            raise FileNotFoundError(base_dir)

    def tweet_path(self, tweet_id, user_id=None):
        path = self.base_dir
        if self.by_user:
            if user_id is None:
                raise TypeError('user_id required')
            path /= user_id

        return path / (tweet_id + '.json')

    def is_stashed(self, tweet_id, user_id=None):
        return self.tweet_path(tweet_id, user_id=user_id).exists()

    def stash(self, tweet, overwrite=False):
        tweet_id = tweet['id_str']
        user_id = tweet['user']['id_str']
        if overwrite or not self.is_stashed(tweet_id, user_id=user_id):
            tweet_path = self.tweet_path(tweet_id, user_id=user_id)
            if self.by_user:
                tweet_path.parent.mkdir(exist_ok=True)
            with tweet_path.open('w') as file:
                json.dump(tweet, file)

    def unstash(self, tweet_id, user_id=None):
        with self.tweet_path(tweet_id, user_id=user_id).open() as file:
            return json.load(file)

    def all_ids(self, user_id=None):
        if not self.by_user:
            paths = self.base_dir.glob('*.json')
        elif user_id is not None:
            paths = (self.base_dir / user_id).glob('*.json')
        else:
            paths = self.base_dir.glob('*/*.json')

        for tweet_path in paths:
            yield tweet_path.stem
