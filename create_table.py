import sqlite3

db_filename = 'tweets.db'
conn = sqlite3.connect(db_filename)
cur = conn.cursor()
cur.execute(""" 
CREATE TABLE IF NOT EXISTS tweets 
    ( id TEXT PRIMARY KEY
    , text TEXT NOT NULL
    );
DELETE * FROM tweets;
""")
conn.commit()
conn.close()