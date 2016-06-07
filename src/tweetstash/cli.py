"""tweetstash

Usage:
  tweetstash [options] search
  tweetstash [options] listen

Options:
  --config=<config-dir>   Where to look for api.auth and hashtags.list.
  --stash=<stash-dir>     Where to save tweets.
  --by-user               Organize tweets by user id.
  --days=<days>           How far back to search.

"""
import sys
from docopt import docopt

from tweetstash import __version__, FileStash, TweetSearch


def main(argv=sys.argv):
    args = docopt(__doc__, version=__version__)

    stash = FileStash(
        base_dir=args['--stash'] or 'tweets',
        create_dir=True,
        by_user=args['--by-user'],
    )
    search = TweetSearch.from_config_dir(stash, args['--config'] or 'config')

    if args['search']:
        if args['--days']:
            stop_after = {'days': int(args['--days'])}
        else:
            stop_after = {}
        search.search(**stop_after)

    if args['listen']:
        search.listen()
