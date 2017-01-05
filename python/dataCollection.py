import pymongo
import json
import sys
from time import time

from pymongo import MongoClient
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import GaussianNB
sys.path.append("./")
from preprocessor import text_preprocess


client = MongoClient('localhost', 27017)
crawler_db = client.get_database('data-Crawler')

features = []
labels = []
for document in crawler_db.crawlCollection.aggregate([{"$project":{'label':'$data.category','features' :{"$concat":['$data.title','$data.body']},"_id":0}}]):
    new_document = {}
    storing_document = {}
    item_dict = document
    for key, value in item_dict.iteritems():
        #print key
        new_key = key.encode('ascii')
        new_document[new_key] = item_dict[key].encode('ascii', 'ignore')
        if(type(item_dict[key]) is str):
            print 'Type String'
        else:
            new_document[new_key] = item_dict[key]

    features.append(new_document['features'])
    labels.append(new_document['label'].encode('ascii'))




### features_train and features_test are the features for the training
### and testing datasets, respectively
### labels_train and labels_test are the corresponding item labels
features_train, features_test, labels_train, labels_test = text_preprocess(features,labels)




#########################################################
### your code goes here ###
clf = GaussianNB()
t0 = time()
clf.fit(features_train, labels_train)
print "training time:", round(time()-t0, 3), "s"
t1 = time()
pred = clf.predict(features_test)
print "prediction time:", round(time()-t1,3), "s"
y_true = labels_test
y_pred = pred
accuracy = accuracy_score(y_true,y_pred) ##this needs to be taught with more depth. --> accuracy is calculated on what basis. How does the graph come into play. 
print(accuracy)
