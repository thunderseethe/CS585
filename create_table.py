import sqlite3

db_filename = 'tweets.db'
conn = sqlite3.connect(db_filename)
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS tweets;")
cur.execute(""" 
CREATE TABLE tweets 
    ( id TEXT PRIMARY KEY
    , text TEXT NOT NULL
    , in_reply_to_screen_name TEXT
    , in_reply_to_status_id TEXT
    , in_reply_to_user_id TEXT
    , retweeted INTEGER
    , retweeted_status_id TEXT
    );
""")

cur.execute("DROP TABLE IF EXISTS hashtags;")
cur.execute(""" 
CREATE TABLE hashtags
    ( id TEXT
    , hashtag TEXT
    , start_index INTEGER
    , end_index INTEGER
    );
""")

conn.commit()
conn.close()