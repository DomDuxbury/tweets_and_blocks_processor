import datetime
import logging

import trollius as asyncio
import pandas as pd

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sklearn.externals import joblib

import postgres
import tweets

def main():
    
    logging.basicConfig()
    db = postgres.Connection()
    tc = tweets.Classifier() 
    model = joblib.load('./models/price_pipeline.pkl')
    
    start_scheduler(db, tc, model)
        

def start_scheduler(db, tc, model):
    
    sched = AsyncIOScheduler()
    sched.daemonic = False
    sched.add_job(pipeline, 'cron', second=0, args = [db, tc, model])
    sched.start()
    
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        db.conn.close()
        pass

def get_aggregate_tweets(db, tc): 
    
    query = """SELECT Status
               FROM tweets
               WHERE created_at > ( NOW() - INTERVAL '60 minute' )"""
    
    tweets_last_hour = db.query(query)
    tweets_last_hour = pd.DataFrame(tweets_last_hour, columns = ["status"])

    labelled_tweets = tc.classify(tweets_last_hour)

    aggregate = labelled_tweets.groupby(["label"]).count()

    return aggregate

def get_volume_low_high(db):

    query = """SELECT
                    MIN(low) as low, 
                    MAX(high) as high, 
                    SUM(volume) as volume
                FROM (
                    SELECT
                	EXTRACT(hour from timestamp) AS hour, 
                        EXTRACT(minute from timestamp) AS minute,
                        AVG(open) as open, 
                        AVG(close) as close, 
                        AVG(low) as low, 
                        AVG(high) as high,
                        SUM(volume) as volume
                    FROM market_price
                    WHERE timestamp > ( NOW() - INTERVAL '60 minute' )
                    AND volume > 0
                    GROUP BY hour, minute
                    ORDER BY hour, minute) as minutes"""
    
    return db.query(query)[0]

def get_last_close_price(db):

    query = """SELECT close
               FROM (SELECT
               	EXTRACT(hour from timestamp) AS hour, 
                   EXTRACT(minute from timestamp) AS minute,
                   AVG(open) as open, 
                   AVG(close) as close, 
                   AVG(low) as low, 
                   AVG(high) as high,
                   SUM(volume) as volume
               FROM market_price
               WHERE timestamp > ( NOW() - INTERVAL '60 minute' )
               AND volume > 0
               GROUP BY hour, minute
               ORDER BY hour DESC, minute DESC) as minutes
               LIMIT 1"""

    return db.query(query)[0][0]

def get_open_price(db):

    query = """SELECT open
               FROM (SELECT
               	EXTRACT(hour from timestamp) AS hour, 
                   EXTRACT(minute from timestamp) AS minute,
                   AVG(open) as open, 
                   AVG(close) as close, 
                   AVG(low) as low, 
                   AVG(high) as high,
                   SUM(volume) as volume
               FROM market_price
               WHERE timestamp > ( NOW() - INTERVAL '60 minute' )
               AND volume > 0
               GROUP BY hour, minute
               ORDER BY hour ASC, minute ASC) as minutes
               LIMIT 1"""

    return db.query(query)[0][0]
 

def pipeline(db, tc, model):

    tweets = get_aggregate_tweets(db, tc).as_matrix()

    open_price = get_open_price(db)
    close_price = get_last_close_price(db)
    low_price, high_price, volume = get_volume_low_high(db)

    now = datetime.datetime.now()
    
    recent_data = [[
        now.hour, now.minute,
        tweets[1][0], tweets[1][0], tweets[2][0],
        open_price, close_price, 
        low_price, high_price, 
        volume
    ]]

    print model.predict(recent_data)

if __name__ == "__main__":
    main()
