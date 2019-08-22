# Databricks notebook source
# Databricks notebook source
import numpy as np
import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

nltk.download('stopwords')

# COMMAND ----------

dataset = pd.read_csv("https://storageacoountrd.blob.core.windows.net/rdcontainer/train.csv")
# print(dataset.head(50))

# COMMAND ----------

corpus = []
for i in range(len(dataset)):
  review = re.sub('[^a-zA-Z]',' ',dataset["SentimentText"][i])
  review = review.lower()
  review = review.split()  
  ps = PorterStemmer()  
  review = [ps.stem(word) for word in review if word not in set(stopwords.words("english"))]
  review = ' '.join(review)
  corpus.append(review)
# print(corpus)

# COMMAND ----------

# Creating the bag of Words model
from sklearn.feature_extraction.text import CountVectorizer
cv = CountVectorizer(max_features=1500)
X = cv.fit_transform(corpus).toarray()
y = dataset.iloc[:,1].values

# COMMAND ----------

# Fitting the Decision Tree Classifier in Training set
from sklearn.tree import DecisionTreeClassifier
classifier = DecisionTreeClassifier(criterion="entropy", random_state=0)
classifier.fit(X,y)

# COMMAND ----------

import pickle
filename = 'twitter_model.sav'
pickle.dump(classifier,open(filename,'wb'))


# COMMAND ----------

from azure.storage.blob import BlockBlobService, PublicAccess

block_blob_service = BlockBlobService(account_name='storageacoountrd', account_key='QHWKpqdxjSedBX0Ng82Y7sPd5omdbgrB+MuimsHZ+slRcLQwmYS/iV1t3qQvcyXVbucCQgBBcpLfy5agrOG50g==')

container_name ='rdcontainer'
block_blob_service.create_container(container_name)

# Set the permission so the blobs are public.
block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)

# Upload the created file, use local_file_name for the blob name
block_blob_service.create_blob_from_path(container_name, filename, filename)


# COMMAND ----------
