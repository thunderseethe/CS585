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

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.count = 0

    def on_error(self, status_code):
        if status_code == 420:
            return False

    def on_status(self, status):
        self.queue.append((status.id_str, status.text))
        self.count += 1
        print("%d:\t%s" % (self.count, status.text))

db_filename = 'tweets.db'
queue = deque()

myStreamListener = MyStreamListener(queue)
myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
myStream.sample(async=True)

while len(queue) != 0:
    tweet = queue.pop()
    cur.execute("INSERT INTO tweets VALUES (?, ?)", tweet)

conn.commit()
conn.close()