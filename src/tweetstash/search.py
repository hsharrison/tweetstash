import sys
from pathlib import Path
from datetime import timedelta
from toolz import partition_all
import tweepy
from tqdm import tqdm

tweets_per_query = 100  # max allowed by Twitter API
max_hashtags_per_search = 30


class TweetSearch:
    def __init__(self, stash, auth_data, search_terms):
        self.stash = stash
        self.auth_data = auth_data
        self.search_terms = search_terms

    @classmethod
    def from_config_dir(cls, stash, config_dir=None):
        config_path = Path(config_dir or '.')

        # *.auth
        try:
            auth_path = next(config_path.glob('*.auth'))
        except StopIteration:
            raise FileNotFoundError('No .auth file found in {}'.format(config_path.absolute()))
        with auth_path.open() as auth_file:
            auth_data = auth_file.read().splitlines()

        # hashtags.list
        hashtags_path = config_path / 'hashtags.list'
        if not hashtags_path.is_file():
            raise FileNotFoundError(hashtags_path)
        with hashtags_path.open(encoding='utf-8') as hashtags_file:
            hashtags = hashtags_file.read().splitlines()
        search_terms = ['#' + hashtag for hashtag in hashtags]

        return cls(stash, auth_data, search_terms)

    def search_api(self):
        auth = tweepy.AppAuthHandler(*self.auth_data[:2])
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        if not api:
            raise ValueError('Authentication failed')
        return api

    def stream_auth(self):
        auth = tweepy.OAuthHandler(*self.auth_data[:2])
        auth.set_access_token(*self.auth_data[2:])
        return auth

    def search(self, **stop_after):
        if not stop_after:
            stop_after['days'] = 36500
        stop_delta = timedelta(**stop_after)

        for hashtags in partition_all(max_hashtags_per_search, self.search_terms):
            tweets = search_twitter(self.search_api(), query=' OR '.join(hashtags))
            try:
                tweet = next(tweets)
            except StopIteration:
                break
            stop_time = tweet.created_at - stop_delta
            self.stash.stash(tweet._json)

            for tweet in tweets:
                if tweet.created_at < stop_time:
                    break
                self.stash.stash(tweet._json)

    def listen(self):
        listener = StashListener(self.stash)
        stream = tweepy.Stream(self.stream_auth(), listener)

        print('Listening (press Ctrl-C to stop)...')
        try:
            stream.filter(track=self.search_terms)
        except KeyboardInterrupt:
            sys.exit()


class StashListener(tweepy.streaming.StreamListener):
    def __init__(self, stash, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stash = stash

    def on_status(self, tweet):
        self.stash.stash(tweet._json)
        return True

    def on_error(self, status):
        print(status, file=sys.stderr)
        if status == 420:
            raise RuntimeError('Rate limited')


def search_twitter(api, max_results=None, progress=False, **query):
    """Perform a twitter search.

    See http://docs.tweepy.org/en/latest/api.html#API.search

    Parameters
    ----------
    api : tweepy.API
    max_results : int, optional
        The maximum number of tweets to return.
        Default is None (unlimited).
    progress : bool, optional
        Whether to show a progress bar.
    query : str, optional
        Query string.
    lang : str, optional
        Restrict tweets by language, given by an ISO 639-1 code.
    since_id : int, optional
        Return only statuses with ID greater than (i.e., more recent than) the specified ID.
    max_id : int, optional
        Return only statuses with ID less than (i.e., older than) the specified ID.
    geocode : str, optional
        Returns tweets by users located within a given radius.
        The parameter is specificed by 'latitude,longitude,radius' with radius followed by either 'km' or 'mi'.
        Note: see https://twittercommunity.com/t/search-api-returning-very-sparse-geocode-results/27998

    Yields
    ------
    tweepy.models.Status

    """
    query['q'] = query.pop('query')

    n_tweets_found = 0
    max_id = query.get('max_id', -1)
    if progress:
        progress_bar = tqdm(total=max_results, unit='tweets')
    while not max_results or n_tweets_found < max_results:
        try:
            if max_id > 0:
                query['max_id'] = str(max_id - 1)

            new_tweets = api.search(count=tweets_per_query, **query)
            if not new_tweets:
                break

            if progress:
                progress_bar.update(len(new_tweets))
            n_tweets_found += len(new_tweets)
            max_id = new_tweets[-1].id

            yield from new_tweets

        except KeyboardInterrupt:
            break

    if progress:
        progress_bar.close()
