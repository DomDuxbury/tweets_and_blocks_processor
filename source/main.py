import pandas as pd
import tweets
import postgres

def main():
    
    db = postgres.Connection()
    tc = tweets.Classifier() 

    query = """SELECT Status 
               FROM tweets
               WHERE created_at > ( NOW() - INTERVAL '1 hour' )"""
    
    tweets_last_hour = db.query(query)
    tweets_last_hour = pd.DataFrame(tweets_last_hour, columns = ["status"])

    labelled_tweets = tc.classify(tweets_last_hour)
    aggregate = labelled_tweets.groupby(["label"]).count()

    print aggregate

if __name__ == "__main__":
    main()
