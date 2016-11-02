import tweepy
import sqlite3
from collections import deque

consumer_token = 'dlJlSRRqz4YAhNaMD7lmYownr'
consumer_secret = 'NcjxohFcSLKkxMhgOPDhbVtfuinyXf5imbpfsLfGjWbPwCEdf7'

access_token = '41887731-ZUOE6iV6pYjM6FrtGzWLG9Nx2az5QAi9FKjtpt69u'
access_secret = 'bZzmDscpQHe5UyKszFtjqZhzEMIRhwWaa9CKkRaUdwwmJ'

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

def _get(obj, prop):
    """Poor man's maybe monad"""
    return getattr(obj, prop) if obj != None and hasattr(obj, prop) else None

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.poison_pill = ((), ())
        self.count = 0

    def on_error(self, status_code):
        if status_code == 420:
            self.queue.append(self.poison_pill) 
            return False

    def on_status(self, status):
        tweet = (status.id, status.text, status.in_reply_to_screen_name, status.in_reply_to_status_id, status.in_reply_to_user_id, status.retweeted, _get(_get(status, 'retweeted_status'), 'id'))
        hashtags = [(status.id, h['text'], h['indices'][0], h['indices'][1]) for h in status.entities['hashtags']]
        self.queue.append((tweet, hashtags))
        self.count += 1
        print("%d:\t%s" % (self.count, status.text))

db_filename = 'tweets.db'
conn = sqlite3.connect(db_filename)
cur = conn.cursor()
queue = deque()

myStreamListener = MyStreamListener(queue)
myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
myStream.sample(async=True)

while len(queue) != 0:
    (tweet, hashtags) = queue.pop()
    if not tweet and not hashtags:
        break #empty tuples denote poison pill

    cur.execute("INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?)", tweet)
    cur.executemany('INSERT INTO hashtags (id, hashtag, start_index, end_index) VALUES (?, ?, ?, ?)', hashtags)

    conn.commit()

conn.close()