# Databricks notebook source
import numpy as np
import pandas as pd

import re
import nltk

from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
nltk.download('stopwords')

# COMMAND ----------

dataset=pd.read_csv("https://storageacoountrd.blob.core.windows.net/rdcontainer/test.csv")

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

# COMMAND ----------

# Creating the bag of Words model
from sklearn.feature_extraction.text import CountVectorizer
cv = CountVectorizer(max_features=1500)
X_test = cv.fit_transform(corpus).toarray()
y_test = dataset.iloc[:,1].values

# COMMAND ----------

from azure.storage.blob import BlockBlobService, PublicAccess

block_blob_service = BlockBlobService(account_name='storageacoountrd', account_key='QHWKpqdxjSedBX0Ng82Y7sPd5omdbgrB+MuimsHZ+slRcLQwmYS/iV1t3qQvcyXVbucCQgBBcpLfy5agrOG50g==')

container_name ='rdcontainer'

# COMMAND ----------

import pickle

filename='twitter_model.sav'
filename1='test.sav'
block_blob_service.get_blob_to_path(container_name,filename,filename1)
loaded_model = pickle.load(open(filename1, 'rb'))
result = loaded_model.predict(X_test)
print(result)

# COMMAND ----------



# COMMAND ----------


