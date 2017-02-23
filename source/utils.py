from __future__ import division
import pandas as pd
import numpy as np
import nltk.classify.util
from nltk import ngrams
from nltk.classify import NaiveBayesClassifier
from nltk.classify import DecisionTreeClassifier 
 
def extract_tweet_features(tweets, ngrams):
    features = []

    for index, row in tweets.iterrows():
        
        ngram_features = extract_ngram_features(row["status"], ngrams)
        features.append((ngram_features, row["label"]))
        
    return features

def extract_ngram_features(text, n):
    features = {}
    for nextN in range(1,n+1):
        for gram in ngrams(text.split(), nextN):
            features[gram] = True
    return features

def classifyDataframe(classifier, df, ngrams):
    features = []
    for index, row in df.iterrows():

        ngram_features = extract_ngram_features(row["status"], ngrams)
        features.append(ngram_features)
        
    labels = classifier.classify_many(features)
    df["label"] = labels
    return df
