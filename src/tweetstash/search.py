from pathlib import Path
from datetime import timedelta
import tweepy
from tqdm import tqdm

tweets_per_query = 100  # max allowed by Twitter API


def limit_tweet_age(tweets, stop_time=None, stop_delta=None, **stop_delta_kwargs):
    """Stop a stream of historical tweets after a certain time.

    Parameters
    ----------
    tweets : sequence of tweepy.Tweet
    stop_time : datetime.datetime, optional
        The stop time specified as an absolute time.
    stop_delta : datetime.timedelta, optional
        The stop time specified as a relative time.
    **stop_delta_kwargs
        The most natural way to specify the relative time, as datetime.timedelta kwargs.
        E.g., days, weeks.

    Yields
    ------
    tweepy.Tweet

    """

    if stop_time is None:
        if stop_delta is None:
            stop_delta = timedelta(**stop_delta_kwargs)

        try:
            tweet = next(tweets)
        except StopIteration:
            return
        stop_time = tweet.created_at - stop_delta
        yield tweet

    for tweet in tweets:
        if tweet.created_at < stop_time:
            break
        yield tweet


def search_from_config_dir(config_dir=None):
    """Read the query and auth data from a config dir, and execute the search.

    Parameters
    ----------
    config_dir : str, optional
        The directory to look in.
        The default is the current directory.

    Yields
    ------
    tweepy.Tweet

    """
    if config_dir is None:
        config_dir = Path('.')

    api = read_auth(config_dir=config_dir)
    query = read_query(config_dir=config_dir)

    yield from search_twitter(api, **query)


def read_auth(config_dir=None):
    """Set up auth from a confg file.

    The first file in `config_dir` matching ``*.auth`` will be read.
    This file should contain the consumer key on the first line
    and the consumer secret on the second line.

    Parameters
    ----------
    config_dir : str, optional
        The directory to look in.
        The default is the current directory.

    Returns
    -------
    tweepy.API

    """
    if config_dir is None:
        config_dir = Path('.')

    try:
        auth_file = next(Path.glob('*.auth'))
    except StopIteration:
        raise FileNotFoundError('No .auth file found in {}'.format(config_dir.absolute()))

    with auth_file.open() as auth_file:
        auth_data = auth_file.read()

    auth = tweepy.AppAuthHandler(*auth_data.split('\n'))
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    if not api:
        raise ValueError('Authentication failed')
    return api


def read_query(config_dir=None):
    """Construct a query from a config directory.

    The following files are read:
      - ``hashtags.list``: A list of hashtags to search for
        (one per line, without the ``#``).

    Parameters
    ----------
    config_dir : str, optional
        The directory to look in.
        The default is the current directory.

    Returns
    -------
    dict

    """
    if config_dir is None:
        config_dir = Path('.')

    query = {}

    # hashtags.list
    hashtag_path = config_dir / 'hashtags.list'
    if hashtag_path.is_file():
        with hashtag_path.open() as hashtag_file:
            hashtags = hashtag_file.readlines()

        query['query'] = ' OR '.join('#' + hashtag for hashtag in hashtags)

    return query


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
    tweepy.Tweet

    """
    query['query'] = query.get('q')

    n_tweets_found = 0
    max_id = query.get('max_id', -1)
    if progress:
        progress_bar = tqdm(total=max_results, unit='tweets')
    while not max_results or n_tweets_found < max_results:
        try:
            if max_id > 0:
                query['max_id'] = str(max_id - 1)

            try:
                new_tweets = api.search(count=tweets_per_query, **query)
            except tweepy.TweepError as e:
                print('Encountered error: {}'.format(e))
                break

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
