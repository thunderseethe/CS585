import tweepy
import sqlite3
from asyncio import Queue
import asyncio
import twitter
import threading

def _get(obj, prop):
    """Poor man's maybe monad"""
    return getattr(obj, prop) if obj != None and hasattr(obj, prop) else None

#this is bad but only for debug


"""
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.poison_pill = ((), ())
        self.count = 0

    def on_error(self, status_code):
        if status_code == 420:
            self.queue.put_nowait(self.poison_pill) 
            return False

    def on_status(self, status):
        tweet = (status.id, status.text, status.in_reply_to_screen_name, status.in_reply_to_status_id, status.in_reply_to_user_id, status.retweeted, _get(_get(status, 'retweeted_status'), 'id'))
        hashtags = [(status.id, h['text'], h['indices'][0], h['indices'][1]) for h in status.entities['hashtags']]
        self.queue.put_nowait((tweet, hashtags))
        self.count += 1
"""

# poison_pill = (None, None)

# async def fetch(api, queue, lock):
#     with lock:
#         try:
#             res = api.GetStreamSample()
#         except twitter.TwitterError as error:
#             return (None, None)

#     tweet = (res.id, res.text, res.in_reply_to_screen_name, res.in_reply_to_status_id, status.in_reply_to_user_id, res,retweeted, _get(_get(res, 'retweeted_status'), 'id'))
#     hashtags = [(res.id, h['text'], h['indices'][0], h['indices'][1]) for h in res.entities['hashtags']]
#     return (tweet, hashtags)
    


if __name__ == '__main__':
    consumer_token = 'dlJlSRRqz4YAhNaMD7lmYownr'
    consumer_secret = 'NcjxohFcSLKkxMhgOPDhbVtfuinyXf5imbpfsLfGjWbPwCEdf7'

    access_token = '41887731-ZUOE6iV6pYjM6FrtGzWLG9Nx2az5QAi9FKjtpt69u'
    access_secret = 'bZzmDscpQHe5UyKszFtjqZhzEMIRhwWaa9CKkRaUdwwmJ'


    api = twitter.Api(consumer_key=consumer_token, consumer_secret=consumer_secret, access_token_key=access_token, access_token_secret=access_secret)

    db_filename = 'tweets.db'    
    conn = sqlite3.connect(db_filename)
    cur = conn.cursor()
    

    #(tweet, hashtags) = await queue.get()
    
    for res in api.GetStreamSample():
        #try:
        #    res = api.GetStreamSample()
        #except TwitterError as error:
        #    print(error)
        #    break

        if('delete' in res or 'id' not in res):
            continue
        print('%d: %s' % (res['id'], res['text']))
        tweet = (res['id'], res['text'], res['in_reply_to_screen_name'], res['in_reply_to_status_id'], res['in_reply_to_user_id'], res['retweeted'], _get(_get(res, 'retweeted_status'), 'id'))
        hashtags = [(res['id'], h['text'], h['indices'][0], h['indices'][1]) for h in res['entities']['hashtags']]
        try:
            cur.execute("INSERT INTO tweets VALUES (?, ?, ?, ?, ?, ?, ?)", tweet)
            cur.executemany('INSERT INTO hashtags (id, hashtag, start_index, end_index) VALUES (?, ?, ?, ?)', hashtags)
        except sqlite3.IntegrityError:
            print(id, "UNIQUE constraint violated")

        #(tweet, hashtags) = await queue.get()
        conn.commit()

    conn.close()