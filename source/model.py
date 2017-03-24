from sklearn.externals import joblib

class PriceModel():

    def __init__(self):

        self.classifier = joblib.load('./models/price_pipeline.pkl')

    def predict(self, data):

        predictions = self.classifier.predict(data)
        return predictions[0] 
