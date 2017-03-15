import postgres
import tweets
import datetime
import trollius as asyncio
import pandas as pd
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

def main():
    
    logging.basicConfig()
    db = postgres.Connection()
    tc = tweets.Classifier() 
    start_scheduler(db, tc)
        

def start_scheduler(db, tc):
    
    sched = AsyncIOScheduler()
    sched.daemonic = False
    sched.add_job(process_tweets, 'cron', second=0, args = [db, tc])
    sched.start()
    
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        db.conn.close()
        pass

def process_tweets(db, tc): 
    
    query = """SELECT Status, created_at
               FROM tweets
               WHERE created_at > ( NOW() - INTERVAL '1 minute' )"""
    
    tweets_last_min = db.query(query)
    tweets_last_min = pd.DataFrame(tweets_last_min, columns = ["status", "created_at"])

    # labelled_tweets = tc.classify(tweets_last_min)

    print tweets_last_min

if __name__ == "__main__":
    main()
