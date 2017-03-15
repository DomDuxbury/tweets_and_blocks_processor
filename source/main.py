import pandas as pd
import tweets
import postgres

def main():
    
    db = postgres.Connection()
    tc = tweets.Classifier() 

    query = """SELECT Status
               FROM tweets
               WHERE created_at > ( NOW() - INTERVAL '1 minute' )"""
    
    tweets_last_min = db.query(query)
    tweets_last_min = pd.DataFrame(tweets_last_min, columns = ["status"])

    labelled_tweets = tc.classify(tweets_last_min)
    aggregate = labelled_tweets.groupby(["label"]).count()

    print aggregate

if __name__ == "__main__":
    main()
