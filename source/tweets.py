from sklearn.externals import joblib
import utils

class Classifier():

    def __init__(self):

        self.spam_classifier = joblib.load('./models/spam_classifier.pkl')
        self.pos_neg_classifier = joblib.load('./models/pos_neg_classifier.pkl')

    def classify(self, tweets):
        
        split = split_spam_leg(tweets, self.spam_classifier)

        pos_neg_labelled_tweets = label_tweets(split["leg"], self.pos_neg_classifier)

        return pos_neg_labelled_tweets.append(split["spam"])


def split_spam_leg(tweets, spam_classifier):

    tweets = label_tweets(tweets, spam_classifier)
    groups = tweets.groupby("label")

    return {
        "spam": tweets.loc[groups.groups["spam"]],
        "leg": tweets.loc[groups.groups["leg"]]
    }

def label_tweets(tweets, classifier):
    tweets["label"] = "N/A"
    return utils.classifyDataframe(classifier, tweets, 2)
