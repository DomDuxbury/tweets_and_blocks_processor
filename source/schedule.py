import postgres
import datetime
import trollius as asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler

def main():
    
    logging.basicConfig()
    db = postgres.Connection()
    start_scheduler(db)
        

def start_scheduler(db):
    
    sched = AsyncIOScheduler()
    sched.daemonic = False
    sched.add_job(process_tweets, 'cron', second=0, args = [db])
    sched.start()
    
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

def process_tweets(db): 
    
    result = db.query("SELECT COUNT(*) FROM tweets")
    print result[0][0]


if __name__ == "__main__":
    main()
